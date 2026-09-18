# -*- coding: utf-8 -*-
"""export_pipeline.py

ExportPipeline — the BigQuery-only workflow that aggregates every
`stg_*_tbl` into one gap-filled long table for QA, and exports it to
GCS as a single Parquet file.

This runs strictly *after* `StagePipeline` has already built/refreshed
the `stg_*` tables — this class never writes to a `stg_*` table, it
only reads from them. It shares `cfg_stage_tbl` with `StagePipeline`
(same table, read-only here too) but does not touch its `status`
column and does not create or alter it.

Three steps, always run as a full rebuild (no incremental mode — this
is expected to run only when an indicator is added/changed in
`cfg_stage_tbl` or an upstream `stg_*_tbl` is reconstructed, not on a
tight schedule):

1. `build_long_view()`  — CREATE OR REPLACE VIEW `exp_stg_all_long_vw`
                           as the UNION ALL of every `stg_*_tbl` whose
                           `afs_source_code` appears (non-null, any
                           status) in `cfg_stage_tbl`. Before unioning,
                           every discovered `stg_*_tbl`'s column names
                           are compared against each other; a mismatch
                           raises with a per-table diff so a schema
                           drift on one source is easy to trace back to
                           that specific table.

2. `build_final_table()` — CREATE OR REPLACE TABLE `exp_stg_all_long_tbl`,
                           clustered by (afs_m49_code, afs_indicator_uid).
                           Built from a scaffold (every m49_code x year
                           x ACTIVE/REVIEW-indicator combination) FULL
                           JOINed against the long view, so:
                             - a combination with no matching data comes
                               through as a row with NULL value columns
                               (LEFT-JOIN-style gap filling, for finding
                               "missing data" vs. "broken query"), and
                             - a long-view row with no matching scaffold
                               combination (wrong m49_code, year outside
                               [start_year, current_year], or an
                               indicator no longer ACTIVE/REVIEW) still
                               shows up instead of being silently
                               dropped, for finding stale/orphaned data.
                           The three join keys are COALESCEd from both
                           sides so an orphaned long-view-only row still
                           carries a non-NULL key to sort/cluster by.

3. `export_to_gcs()`     — EXPORT DATA to a single Parquet file at a
                           fixed URI (no wildcard), overwrite=true, so
                           the same GCS URL is reused on every run.
                           CLUSTER BY only controls physical storage
                           inside BigQuery — it does not guarantee row
                           order in query output — so ORDER BY is
                           re-applied explicitly at export time too.

`run_all()` chains all three. Unlike `StagePipeline.update()` /
`reconstruct()`, whose per-source groups are independent (one group's
failure doesn't block another), these three steps are strictly
sequential — each consumes the previous step's output — so `run_all()`
stops at the first failure instead of continuing.

Nothing is executed until `confirm_apply=True` is passed explicitly, on
either the individual step methods or `run_all()`; calling any of them
without it returns a preview of the SQL/plan for review before
committing.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError


REQUIRED_CFG_STAGE_COLUMNS = {
    "afs_uid", "afs_id", "afs_source_code", "status",
    "afs_outcome_code", "afs_outcome_name",
    "afs_theme_code", "afs_theme_name",
    "afs_subtheme_code", "afs_subtheme_name",
    "afs_indicator_code", "afs_indicator_name",
}

REQUIRED_SCOPE_COLUMNS = {"m49_code", "area_name"}

# Which cfg_stage_tbl statuses count as "currently live" for the
# indicator scaffold (step 2) and the taxonomy-consistency check.
# NOT used for stg_tbl discovery (step 1) — that step takes every
# non-null afs_source_code regardless of status, since a stg_tbl can
# hold real data even for an indicator that isn't ACTIVE/REVIEW right
# now, and the long view is meant to reflect everything physically
# present, not just what's currently reviewed.
ACTIVE_STATUSES = ["ACTIVE", "REVIEW"]

# Same cap as StagePipeline — keeps one long BQ error message from
# blowing up a log table row.
MAX_ERROR_MESSAGE_LEN = 2000

STEP_BUILD_LONG_VIEW = "BUILD_LONG_VIEW"
STEP_BUILD_FINAL_TABLE = "BUILD_FINAL_TABLE"
STEP_EXPORT_GCS = "EXPORT_GCS"


class ExportPipeline:
    """The BigQuery-only pipeline that aggregates stg_*_tbl into one
    gap-filled long table and exports it to GCS as Parquet.

    All reads/writes are scoped to `{project_id}.{dataset_id}`, except
    the GCS export itself. This class only reads `cfg_stage_tbl` and
    the `stg_*_tbl` tables — it never creates or alters either; it owns
    only the long view, the final table, and its own log table.
    """

    def __init__(
        self,
        bq_client: bigquery.Client,
        project_id: str,
        dataset_id: str,
        cfg_stage_table_name: str,
        scope_table_name: str,
        log_table_name: str,
        gcs_uri: str,
        long_view_name: str = "exp_stg_all_long_vw",
        final_table_name: str = "exp_stg_all_long_tbl",
        start_year: int = 2010,
    ):
        self.bq_client = bq_client
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.cfg_stage_table_name = cfg_stage_table_name
        self.scope_table_name = scope_table_name
        self.log_table_name = log_table_name
        self.gcs_uri = gcs_uri
        self.long_view_name = long_view_name
        self.final_table_name = final_table_name
        self.start_year = start_year

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------

    def _table_fqn(self, table_name: str) -> str:
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    @property
    def _cfg_stage_table_fqn(self) -> str:
        return self._table_fqn(self.cfg_stage_table_name)

    @property
    def _scope_table_fqn(self) -> str:
        return self._table_fqn(self.scope_table_name)

    @property
    def _log_table_fqn(self) -> str:
        return self._table_fqn(self.log_table_name)

    @property
    def _long_view_fqn(self) -> str:
        return self._table_fqn(self.long_view_name)

    @property
    def _final_table_fqn(self) -> str:
        return self._table_fqn(self.final_table_name)

    @staticmethod
    def _stg_tbl_name(afs_source_code: str) -> str:
        """Same naming rule as StagePipeline._stg_tbl_name — kept in
        sync deliberately, since these have to resolve to the same
        physical tables StagePipeline builds."""
        return f"stg_{afs_source_code}_tbl"

    @staticmethod
    def _new_run_id() -> str:
        return datetime.now().strftime("%Y%m%d-%H%M%S")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _format_error(exc: Exception) -> str:
        msg = str(exc)
        if len(msg) > MAX_ERROR_MESSAGE_LEN:
            msg = msg[:MAX_ERROR_MESSAGE_LEN] + " …[truncated]"
        return msg

    def _existing_tables(self) -> set:
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        return {t.table_id for t in self.bq_client.list_tables(dataset_ref)}

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _log_table_schema() -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("step", "STRING"),           # BUILD_LONG_VIEW / BUILD_FINAL_TABLE / EXPORT_GCS
            bigquery.SchemaField("object_name", "STRING"),    # view name / table name / gcs uri
            bigquery.SchemaField("job_status", "STRING"),     # SUCCESS / FAILED
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("bq_job_id", "STRING"),
            bigquery.SchemaField("rows_affected", "INTEGER"),
            bigquery.SchemaField("source_stg_tbls", "STRING", mode="REPEATED"),  # only set for BUILD_LONG_VIEW
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]

    def _create_log_table(self) -> None:
        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.project_id, self.dataset_id),
            self.log_table_name,
        )
        table = bigquery.Table(table_ref, schema=self._log_table_schema())
        self.bq_client.create_table(table)
        print(f"Created log table `{self._log_table_fqn}`.")

    def _check_taxonomy_consistency(self) -> pd.DataFrame:
        """Defensive check: if the same afs_uid maps to more than one
        distinct combination of taxonomy fields across its ACTIVE/REVIEW
        cfg_stage_tbl rows (e.g. inconsistent outcome_name spelling
        across src_idx), the scaffold's DISTINCT will keep both
        variants and silently double that indicator's rows. Returns the
        offending afs_uid (empty DataFrame if none)."""
        sql = f"""
        SELECT afs_uid, COUNT(DISTINCT TO_JSON_STRING(STRUCT(
            afs_id, afs_outcome_code, afs_outcome_name,
            afs_theme_code, afs_theme_name,
            afs_subtheme_code, afs_subtheme_name,
            afs_indicator_code, afs_indicator_name
        ))) AS n_variants
        FROM `{self._cfg_stage_table_fqn}`
        WHERE status IN UNNEST(@statuses)
        GROUP BY afs_uid
        HAVING n_variants > 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("statuses", "STRING", ACTIVE_STATUSES)]
        )
        return self.bq_client.query(sql, job_config=job_config).to_dataframe()

    def validate(self) -> None:
        """Confirms cfg_stage_tbl and the scope table exist (this class
        never creates either). Ensures the log table exists, creating
        it if not. Confirms gcs_uri has no wildcard (single-file export
        only). Raises with a per-table diff if the discovered stg_tbl
        don't all share the same columns (see
        _check_schema_consistency). Warns (does not raise) if any
        afs_uid has inconsistent taxonomy fields across cfg rows, since
        that would inflate the scaffold. Safe to call repeatedly;
        build_long_view(), build_final_table() and export_to_gcs()
        don't call this automatically (they're meant to be usable
        standalone during debugging) — only run_all() calls it first.
        """
        existing = self._existing_tables()

        if self.cfg_stage_table_name not in existing:
            raise RuntimeError(
                f"Config table '{self.cfg_stage_table_name}' not found in "
                f"{self.project_id}.{self.dataset_id}. ExportPipeline does not create it."
            )
        print(f"Config table '{self.cfg_stage_table_name}' found.")

        if self.scope_table_name not in existing:
            raise RuntimeError(
                f"Scope table '{self.scope_table_name}' not found in "
                f"{self.project_id}.{self.dataset_id}. ExportPipeline does not create it."
            )
        print(f"Scope table '{self.scope_table_name}' found.")

        if self.log_table_name not in existing:
            print(f"Log table '{self.log_table_name}' not found — creating it.")
            self._create_log_table()
        else:
            print(f"Log table '{self.log_table_name}' found.")

        if "*" in self.gcs_uri:
            raise ValueError(
                f"gcs_uri contains a wildcard ('{self.gcs_uri}'). This pipeline requires a "
                f"single fixed-path export — remove the '*' so the same URL is reused every run."
            )
        print(f"GCS URI '{self.gcs_uri}' has no wildcard — single-file export OK.")

        stg_tables = self._discover_stg_tables()
        self._check_schema_consistency(stg_tables)

        df_dupes = self._check_taxonomy_consistency()
        if not df_dupes.empty:
            print(
                f"WARNING: {len(df_dupes)} afs_uid have inconsistent taxonomy fields across "
                f"cfg_stage_tbl rows (status in {ACTIVE_STATUSES}). This will duplicate those "
                f"indicators' rows in the scaffold. Affected afs_uid: {df_dupes['afs_uid'].tolist()}"
            )
        else:
            print("No afs_uid with inconsistent taxonomy fields found.")

    # ------------------------------------------------------------------
    # Step 1 — discover stg_*_tbl to union (pure builder + a read, no writes)
    # ------------------------------------------------------------------

    def _discover_stg_tables(self) -> List[str]:
        """All non-null, distinct afs_source_code in cfg_stage_tbl —
        status is deliberately ignored here (unlike the indicator
        scaffold, which does filter by ACTIVE_STATUSES): this step's
        job is to find every stg_tbl that physically has data worth
        including in the long view, regardless of where its indicators
        currently sit in the review lifecycle. Each afs_source_code is
        turned into its expected stg_tbl name via the same naming rule
        StagePipeline uses, then only the ones that physically exist
        are kept. A source configured but not yet built (e.g. still
        DRAFT-only, never reconstructed) is skipped with a warning
        rather than failing the whole run.
        """
        sql = f"""
        SELECT DISTINCT afs_source_code
        FROM `{self._cfg_stage_table_fqn}`
        WHERE afs_source_code IS NOT NULL
        """
        df = self.bq_client.query(sql).to_dataframe()

        expected = {
            self._stg_tbl_name(code) for code in df["afs_source_code"].dropna().unique()
        }
        existing = self._existing_tables()

        found = sorted(expected & existing)
        missing = sorted(expected - existing)
        if missing:
            print(
                f"WARNING: {len(missing)} expected stg_tbl not found and will be skipped: {missing}"
            )
        return found

    def _check_schema_consistency(self, stg_tables: List[str]) -> None:
        """UNION ALL matches columns positionally, so every stg_tbl
        going into the long view must have the exact same column names
        in the exact same order — a plain `SELECT *` union will produce
        silently wrong results (or a cryptic BigQuery error) otherwise.

        Fetches each table's column list (one get_table() call per
        table — metadata only, no query cost) and compares them all
        against whichever schema the majority of tables share. Raises
        with a clear per-table diff naming exactly which table(s) are
        the odd one(s) out and how (missing columns, extra columns, or
        same columns in a different order), rather than letting the
        first confusing BigQuery UNION ALL error be the only clue.
        Called from both validate() and build_long_view(), so the
        problem surfaces whichever way the pipeline is run.
        """
        if not stg_tables:
            return

        schemas: Dict[str, tuple] = {
            t: tuple(f.name for f in self.bq_client.get_table(self._table_fqn(t)).schema)
            for t in stg_tables
        }

        counts = Counter(schemas.values())
        baseline_schema, n_agree = counts.most_common(1)[0]
        mismatches = {t: cols for t, cols in schemas.items() if cols != baseline_schema}
        if not mismatches:
            print(f"Schema check OK: all {len(stg_tables)} stg_tbl share the same {len(baseline_schema)} columns.")
            return

        baseline_set = set(baseline_schema)
        lines = [
            f"Schema mismatch across {len(mismatches)}/{len(stg_tables)} stg_tbl. "
            f"Majority schema ({len(baseline_schema)} column(s), shared by {n_agree} table(s)): "
            f"{list(baseline_schema)}"
        ]
        for t, cols in sorted(mismatches.items()):
            col_set = set(cols)
            missing = [c for c in baseline_schema if c not in col_set]
            extra = [c for c in cols if c not in baseline_set]
            detail = f"  - `{t}`: {len(cols)} column(s)"
            if missing:
                detail += f", missing {missing}"
            if extra:
                detail += f", extra {extra}"
            if not missing and not extra:
                detail += f", same columns but different ORDER: {list(cols)}"
            lines.append(detail)

        raise ValueError("\n".join(lines))

    # ------------------------------------------------------------------
    # Step 2 — long view
    # ------------------------------------------------------------------

    def _build_long_view_query(self, stg_tables: List[str]) -> str:
        if not stg_tables:
            raise ValueError("No stg_tbl available to union into the long view.")
        selects = [f"SELECT * FROM `{self._table_fqn(t)}`" for t in stg_tables]
        return "\nUNION ALL\n".join(selects)

    def build_long_view(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """CREATE OR REPLACE VIEW exp_stg_all_long_vw as the UNION ALL
        of every stg_tbl found by _discover_stg_tables(). Always a full
        replace — a view definition, not stored data, so there's no
        incremental mode to speak of. rows_affected is left None here:
        a view holds no data of its own to count.

        Runs _check_schema_consistency() first (even on a dry run —
        it's metadata-only, no query cost) so a column mismatch across
        stg_tbl is caught here, with a per-table diff, instead of
        surfacing later as a confusing UNION ALL error from BigQuery.
        """
        stg_tables = self._discover_stg_tables()
        self._check_schema_consistency(stg_tables)
        union_query = self._build_long_view_query(stg_tables)

        if not confirm_apply:
            print(
                f"[DRY RUN] build_long_view(): would union {len(stg_tables)} stg_tbl "
                f"into `{self.long_view_name}`: {stg_tables}. Set confirm_apply=True to execute."
            )
            return {"stg_tables": stg_tables, "sql": union_query}

        sql = f"CREATE OR REPLACE VIEW `{self._long_view_fqn}` AS\n{union_query}"
        result = self._run_job(sql)
        result["step"] = STEP_BUILD_LONG_VIEW
        result["object_name"] = self.long_view_name
        result["source_stg_tbls"] = stg_tables
        return result

    # ------------------------------------------------------------------
    # Step 3 — scaffold + final table
    # ------------------------------------------------------------------

    def _build_scaffold_query(self) -> str:
        """m49_code x year(start_year..current_year) x ACTIVE/REVIEW
        indicator, sorted by (afs_m49_code, afs_indicator_uid, afs_year)
        as requested. Year upper bound is computed fresh at query time
        (EXTRACT(YEAR FROM CURRENT_DATE())), so it always tracks the
        current year without needing code changes.
        """
        statuses_literal = ", ".join(f"'{s}'" for s in ACTIVE_STATUSES)
        return f"""
        WITH years AS (
          SELECT year
          FROM UNNEST(GENERATE_ARRAY({self.start_year}, EXTRACT(YEAR FROM CURRENT_DATE()))) AS year
        ),
        indicators AS (
          SELECT DISTINCT
            afs_uid AS afs_indicator_uid,
            afs_id,
            afs_outcome_code, afs_outcome_name,
            afs_theme_code, afs_theme_name,
            afs_subtheme_code, afs_subtheme_name,
            afs_indicator_code, afs_indicator_name
          FROM `{self._cfg_stage_table_fqn}`
          WHERE status IN ({statuses_literal})
        )
        SELECT
          scope.m49_code AS afs_m49_code,
          scope.area_name AS afs_area_name,
          years.year AS afs_year,
          indicators.*
        FROM `{self._scope_table_fqn}` AS scope
        CROSS JOIN years
        CROSS JOIN indicators
        ORDER BY afs_m49_code, afs_indicator_uid, afs_year
        """

    def _build_final_table_query(self) -> str:
        """Scaffold FULL JOIN long view. FULL (not LEFT) so a long-view
        row with no matching scaffold combination — wrong m49_code, a
        year outside [start_year, current_year], or an indicator no
        longer ACTIVE/REVIEW — still surfaces instead of being dropped.
        The three join keys are COALESCEd from both sides so a
        long-view-only row still carries a non-NULL sort/cluster key.
        """
        scaffold_query = self._build_scaffold_query()
        return f"""
        SELECT
          COALESCE(scaffold.afs_m49_code, long_vw.afs_m49_code) AS afs_m49_code,
          COALESCE(scaffold.afs_indicator_uid, long_vw.afs_indicator_uid) AS afs_indicator_uid,
          COALESCE(scaffold.afs_year, long_vw.afs_year) AS afs_year,
          scaffold.* EXCEPT (afs_m49_code, afs_indicator_uid, afs_year),
          long_vw.* EXCEPT (afs_m49_code, afs_indicator_uid, afs_year)
        FROM ({scaffold_query}) AS scaffold
        FULL JOIN `{self._long_view_fqn}` AS long_vw
          ON scaffold.afs_m49_code = long_vw.afs_m49_code
          AND scaffold.afs_indicator_uid = long_vw.afs_indicator_uid
          AND scaffold.afs_year = long_vw.afs_year
        ORDER BY afs_m49_code, afs_indicator_uid, afs_year
        """

    def build_final_table(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """CREATE OR REPLACE TABLE exp_stg_all_long_tbl, clustered by
        (afs_m49_code, afs_indicator_uid). Always a full rebuild — no
        incremental mode. rows_affected comes from an extra get_table()
        call after success, same reasoning as StagePipeline: CREATE
        TABLE AS SELECT is DDL, so BigQuery doesn't populate
        num_dml_affected_rows for it.
        """
        final_query = self._build_final_table_query()

        if not confirm_apply:
            print(
                f"[DRY RUN] build_final_table(): would (re)build `{self.final_table_name}` "
                f"clustered by (afs_m49_code, afs_indicator_uid). Set confirm_apply=True to execute."
            )
            return {"sql": final_query}

        sql = (
            f"CREATE OR REPLACE TABLE `{self._final_table_fqn}`\n"
            f"CLUSTER BY afs_m49_code, afs_indicator_uid\nAS\n{final_query}"
        )
        result = self._run_job(sql, count_table=self.final_table_name)
        result["step"] = STEP_BUILD_FINAL_TABLE
        result["object_name"] = self.final_table_name
        return result

    # ------------------------------------------------------------------
    # Step 4 — export to GCS
    # ------------------------------------------------------------------

    def export_to_gcs(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """EXPORT DATA to self.gcs_uri: a single Parquet file (uri has
        no wildcard, checked in validate()), overwrite=true so the same
        URL is reused every run. Re-applies ORDER BY here explicitly —
        CLUSTER BY controls physical storage/pruning inside BigQuery,
        it does not guarantee row order in query output or export
        files. rows_affected reuses the final table's row count (via
        get_table, same as build_final_table) since this export is an
        unfiltered SELECT * and can't change the row count.
        """
        sql = f"""
        EXPORT DATA OPTIONS(
          uri = '{self.gcs_uri}',
          format = 'PARQUET',
          overwrite = true
        ) AS
        SELECT *
        FROM `{self._final_table_fqn}`
        ORDER BY afs_m49_code, afs_indicator_uid, afs_year
        """

        if not confirm_apply:
            print(
                f"[DRY RUN] export_to_gcs(): would export `{self.final_table_name}` to "
                f"'{self.gcs_uri}' (single file, overwrite=True). Set confirm_apply=True to execute."
            )
            return {"gcs_uri": self.gcs_uri, "sql": sql}

        result = self._run_job(sql, count_table=self.final_table_name)
        result["step"] = STEP_EXPORT_GCS
        result["object_name"] = self.gcs_uri
        return result

    # ------------------------------------------------------------------
    # Shared job runner
    # ------------------------------------------------------------------

    def _run_job(self, sql: str, count_table: Optional[str] = None) -> Dict[str, Any]:
        """Runs one BQ job. Never raises — failures are captured and
        returned so the caller (run_all) can decide whether to stop.
        If count_table is given and the job succeeds, rows_affected is
        filled in from that table's current num_rows (see callers for
        why — DDL/EXPORT statements don't populate num_dml_affected_rows).
        """
        result: Dict[str, Any] = {
            "job_status": "FAILED",
            "error_message": None,
            "bq_job_id": None,
            "rows_affected": None,
        }
        job = None
        try:
            job = self.bq_client.query(sql)
            job.result()  # blocks until done; raises on job error
            result["bq_job_id"] = job.job_id
            result["job_status"] = "SUCCESS"
            if count_table:
                table = self.bq_client.get_table(self._table_fqn(count_table))
                result["rows_affected"] = table.num_rows
        except (GoogleAPIError, Exception) as exc:  # noqa: BLE001 - broad on purpose, see StagePipeline
            if job is not None:
                result["bq_job_id"] = job.job_id
            result["error_message"] = self._format_error(exc)
        return result

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def _write_log(self, run_id: str, results: List[Dict[str, Any]]) -> None:
        """One log row per step, success or failure — a full execution
        history, not just an error log. Granularity is per-step here
        (not per-source-group like StagePipeline's log) since these
        three steps are sequential, not independent units of work."""
        if not results:
            return

        now = self._now_iso()
        rows = [
            {
                "run_id": run_id,
                "step": r["step"],
                "object_name": r["object_name"],
                "job_status": r["job_status"],
                "error_message": r.get("error_message"),
                "bq_job_id": r.get("bq_job_id"),
                "rows_affected": r.get("rows_affected"),
                "source_stg_tbls": r.get("source_stg_tbls", []),
                "created_at": now,
            }
            for r in results
        ]

        errors = self.bq_client.insert_rows_json(self._log_table_fqn, rows)
        if errors:
            print(f"WARNING: failed to write {len(errors)} row(s) to {self.log_table_name}: {errors}")

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run_all(self, confirm_apply: bool = False) -> pd.DataFrame:
        """Runs build_long_view -> build_final_table -> export_to_gcs in
        order. Unlike StagePipeline's per-source-group independence,
        these steps are strictly sequential — each depends on the
        previous one's output — so this stops at the first failure
        instead of continuing to the next step. Calls validate() first.

        Nothing is executed until confirm_apply=True; calling this with
        the default runs validate() and returns an empty preview
        (inspect each step's own dry run individually for SQL detail).
        """
        self.validate()

        if not confirm_apply:
            print(
                "[DRY RUN] run_all(): would run build_long_view -> build_final_table -> "
                f"export_to_gcs() -> '{self.gcs_uri}'. Set confirm_apply=True to execute."
            )
            return pd.DataFrame()

        run_id = self._new_run_id()
        results: List[Dict[str, Any]] = []

        print(f"[{run_id}] STEP 1/3: build_long_view...")
        r1 = self.build_long_view(confirm_apply=True)
        results.append(r1)
        print(f"  -> {r1['job_status']}" + (f": {r1['error_message']}" if r1["job_status"] == "FAILED" else ""))
        if r1["job_status"] != "SUCCESS":
            self._write_log(run_id, results)
            print(f"[{run_id}] Stopped after STEP 1 failure.")
            return pd.DataFrame(results)

        print(f"[{run_id}] STEP 2/3: build_final_table...")
        r2 = self.build_final_table(confirm_apply=True)
        results.append(r2)
        print(f"  -> {r2['job_status']}" + (f": {r2['error_message']}" if r2["job_status"] == "FAILED" else ""))
        if r2["job_status"] != "SUCCESS":
            self._write_log(run_id, results)
            print(f"[{run_id}] Stopped after STEP 2 failure.")
            return pd.DataFrame(results)

        print(f"[{run_id}] STEP 3/3: export_to_gcs...")
        r3 = self.export_to_gcs(confirm_apply=True)
        results.append(r3)
        print(f"  -> {r3['job_status']}" + (f": {r3['error_message']}" if r3["job_status"] == "FAILED" else ""))

        self._write_log(run_id, results)
        return pd.DataFrame(results)

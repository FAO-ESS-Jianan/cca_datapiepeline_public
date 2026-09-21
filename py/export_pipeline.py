# -*- coding: utf-8 -*-
"""export_pipeline.py

ExportPipeline — the BigQuery-only workflow that aggregates every
`stg_*_tbl` (indicator-filtered, "processed") and every `stg_*_vw`
(raw, per-source, "unprocessed") into two independent output tables,
plus a shared m49_code x year x indicator scaffold used to gap-fill
the processed one, and exports both output tables to GCS as Parquet.

This runs strictly *after* `StagePipeline` has already built/refreshed
the `stg_*_tbl` tables (and after the `stg_*_vw` views that feed them
already exist) — this class never writes to a `stg_*` table or view,
it only reads from them. It shares `cfg_stage_tbl` with `StagePipeline`
(same table, read-only here too) but does not touch its `status`
column and does not create or alter it.

Six steps, always run as a full rebuild (no incremental mode — this is
expected to run only when an indicator is added/changed in
`cfg_stage_tbl`, or an upstream `stg_*_tbl`/`stg_*_vw` is
reconstructed, not on a tight schedule). `run_all()` runs them in the
order below, which is also their dependency order; unlike
`StagePipeline.update()` / `reconstruct()`, whose per-source groups are
independent, these steps are strictly sequential, so `run_all()` stops
at the first failure instead of continuing:

1. `build_long_view()`      — CREATE OR REPLACE VIEW `exp_stg_all_long_vw`
                               as the UNION ALL of every `stg_{code}_tbl`
                               (indicator-filtered) whose `afs_source_code`
                               appears (non-null, any status) in
                               `cfg_stage_tbl`.

2. `build_src_view()`       — CREATE OR REPLACE VIEW `exp_src_all_vw` as
                               the UNION ALL of every `stg_{code}_vw` (the
                               raw, per-source view `stg_{code}_tbl` itself
                               reads from, before any indicator-level
                               `filter_string` is applied) for the same set
                               of sources.
                               Both union steps compare column names
                               across their discovered tables/views before
                               unioning; a mismatch raises with a
                               per-object diff so a schema drift on one
                               source is easy to trace back to that
                               specific table/view.

3. `build_scaffold_table()` — CREATE OR REPLACE TABLE
                               `exp_ref_scaffold_cyi_tbl`: every m49_code x
                               year x ACTIVE/REVIEW-indicator combination.
                               Materialized on its own (rather than kept
                               as an inline subquery) so it can be
                               inspected directly and is only computed
                               once even though `build_stg_all_table()`
                               needs it.

4. `build_stg_all_table()`  — CREATE OR REPLACE TABLE `exp_stg_all_long_tbl`,
                               clustered by (afs_m49_code,
                               afs_indicator_uid). The scaffold table FULL
                               JOINed against `exp_stg_all_long_vw`, so:
                                 - a combination with no matching data
                                   comes through as a row with NULL value
                                   columns (LEFT-JOIN-style gap filling,
                                   for finding "missing data" vs. "broken
                                   query"), and
                                 - a long-view row with no matching
                                   scaffold combination (wrong m49_code,
                                   year outside [start_year,
                                   current_year], or an indicator no
                                   longer ACTIVE/REVIEW) still shows up
                                   instead of being silently dropped, for
                                   finding stale/orphaned data.
                               The three join keys are COALESCEd from both
                               sides so an orphaned long-view-only row
                               still carries a non-NULL key to
                               sort/cluster by.

5. `build_src_all_table()`  — CREATE OR REPLACE TABLE `exp_src_all_tbl`,
                               clustered the same way. A straight
                               materialization of `exp_src_all_vw` — no
                               scaffold, no gap-filling — so it always
                               holds exactly what's physically present in
                               the raw source views, as a complete,
                               unfiltered reference alongside the
                               gap-filled table.

6. `export_to_gcs()`        — EXPORT DATA for both `exp_stg_all_long_tbl`
                               and `exp_src_all_tbl`, each to its own
                               fixed-path single Parquet file under
                               `gcs_base_uri` (overwrite=true), so the
                               same two GCS URLs are reused on every run.
                               CLUSTER BY only controls physical storage
                               inside BigQuery — it does not guarantee row
                               order in query output — so ORDER BY is
                               re-applied explicitly at export time too.

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
# indicator scaffold (build_scaffold_table) and the taxonomy-consistency
# check. NOT used for stg_tbl/stg_vw discovery — those steps take every
# non-null afs_source_code regardless of status, since a stg_tbl/stg_vw
# can hold real data even for an indicator that isn't ACTIVE/REVIEW
# right now, and both union views are meant to reflect everything
# physically present, not just what's currently reviewed.
ACTIVE_STATUSES = ["ACTIVE", "REVIEW"]

# Same cap as StagePipeline — keeps one long BQ error message from
# blowing up a log table row.
MAX_ERROR_MESSAGE_LEN = 2000

STEP_BUILD_LONG_VIEW = "BUILD_LONG_VIEW"
STEP_BUILD_SRC_VIEW = "BUILD_SRC_VIEW"
STEP_BUILD_SCAFFOLD_TABLE = "BUILD_SCAFFOLD_TABLE"
STEP_BUILD_STG_ALL_TABLE = "BUILD_STG_ALL_TABLE"
STEP_BUILD_SRC_TABLE = "BUILD_SRC_TABLE"
STEP_EXPORT_GCS = "EXPORT_GCS"


class ExportPipeline:
    """The BigQuery-only pipeline that aggregates stg_*_tbl / stg_*_vw
    into two independent output tables and exports both to GCS as
    Parquet.

    All reads/writes are scoped to `{project_id}.{dataset_id}`, except
    the GCS export itself. This class only reads `cfg_stage_tbl`, the
    `stg_*_tbl` tables and the `stg_*_vw` views — it never creates or
    alters any of them; it owns only the two union views, the scaffold
    table, the two output tables, and its own log table.
    """

    def __init__(
        self,
        bq_client: bigquery.Client,
        project_id: str,
        dataset_id: str,
        cfg_stage_table_name: str,
        scope_table_name: str,
        log_table_name: str,
        gcs_base_uri: str,
        long_view_name: str = "exp_stg_all_long_vw",
        stg_all_table_name: str = "exp_stg_all_long_tbl",
        src_view_name: str = "exp_src_all_vw",
        src_table_name: str = "exp_src_all_tbl",
        scaffold_table_name: str = "exp_ref_scaffold_cyi_tbl",
        start_year: int = 2010,
    ):
        self.bq_client = bq_client
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.cfg_stage_table_name = cfg_stage_table_name
        self.scope_table_name = scope_table_name
        self.log_table_name = log_table_name
        self.gcs_base_uri = gcs_base_uri
        self.long_view_name = long_view_name
        self.stg_all_table_name = stg_all_table_name
        self.src_view_name = src_view_name
        self.src_table_name = src_table_name
        self.scaffold_table_name = scaffold_table_name
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
    def _stg_all_table_fqn(self) -> str:
        return self._table_fqn(self.stg_all_table_name)

    @property
    def _src_view_fqn(self) -> str:
        return self._table_fqn(self.src_view_name)

    @property
    def _src_table_fqn(self) -> str:
        return self._table_fqn(self.src_table_name)

    @property
    def _scaffold_table_fqn(self) -> str:
        return self._table_fqn(self.scaffold_table_name)

    @property
    def _stg_all_export_uri(self) -> str:
        return f"{self.gcs_base_uri.rstrip('/')}/{self.stg_all_table_name}.parquet"

    @property
    def _src_all_export_uri(self) -> str:
        return f"{self.gcs_base_uri.rstrip('/')}/{self.src_table_name}.parquet"

    @staticmethod
    def _stg_tbl_name(afs_source_code: str) -> str:
        """Same naming rule as StagePipeline._stg_tbl_name — kept in
        sync deliberately, since these have to resolve to the same
        physical tables StagePipeline builds."""
        return f"stg_{afs_source_code}_tbl"

    @staticmethod
    def _stg_view_name(afs_source_code: str) -> str:
        """Same naming rule as StagePipeline._stg_view_name — the raw,
        per-source view that stg_{code}_tbl itself reads from, before
        any indicator-level filter_string is applied."""
        return f"stg_{afs_source_code}_vw"

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

    def _distinct_source_codes(self) -> List[str]:
        """Every non-null afs_source_code in cfg_stage_tbl — status is
        deliberately ignored (see ACTIVE_STATUSES comment). Shared by
        both _discover_stg_tables() and _discover_stg_views(), since
        they only differ in which naming rule they apply to each code.
        """
        sql = f"""
        SELECT DISTINCT afs_source_code
        FROM `{self._cfg_stage_table_fqn}`
        WHERE afs_source_code IS NOT NULL
        """
        df = self.bq_client.query(sql).to_dataframe()
        return sorted(df["afs_source_code"].dropna().unique().tolist())

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _log_table_schema() -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("step", "STRING"),           # BUILD_LONG_VIEW / BUILD_SRC_VIEW / BUILD_SCAFFOLD_TABLE / BUILD_STG_ALL_TABLE / BUILD_SRC_TABLE / EXPORT_GCS
            bigquery.SchemaField("object_name", "STRING"),    # view name / table name / gcs uri
            bigquery.SchemaField("job_status", "STRING"),     # SUCCESS / FAILED
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("bq_job_id", "STRING"),
            bigquery.SchemaField("rows_affected", "INTEGER"),
            bigquery.SchemaField("source_stg_tbls", "STRING", mode="REPEATED"),  # only set for BUILD_LONG_VIEW / BUILD_SRC_VIEW
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
        it if not. Confirms gcs_base_uri has no wildcard (single-file
        exports only). Raises with a per-table/view diff if the
        discovered stg_tbl don't all share the same columns, and again
        if the discovered stg_vw don't (see _check_schema_consistency).
        Warns (does not raise) if any afs_uid has inconsistent taxonomy
        fields across cfg rows, since that would inflate the scaffold.
        Safe to call repeatedly; the individual build_*()/export_to_gcs()
        methods don't call this automatically (they're meant to be
        usable standalone during debugging) — only run_all() calls it
        first.
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

        if "*" in self.gcs_base_uri:
            raise ValueError(
                f"gcs_base_uri contains a wildcard ('{self.gcs_base_uri}'). This pipeline requires "
                f"single fixed-path exports — remove the '*' so the same URLs are reused every run."
            )
        print(f"GCS base URI '{self.gcs_base_uri}' has no wildcard — single-file exports OK.")

        stg_tables = self._discover_stg_tables()
        self._check_schema_consistency(stg_tables, label="stg_tbl")

        stg_views = self._discover_stg_views()
        self._check_schema_consistency(stg_views, label="stg_vw")

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
    # Discovery + schema consistency (shared by both union steps)
    # ------------------------------------------------------------------

    def _discover_stg_tables(self) -> List[str]:
        """All non-null, distinct afs_source_code in cfg_stage_tbl,
        turned into their expected stg_{code}_tbl name via the same
        naming rule StagePipeline uses, keeping only the ones that
        physically exist. A source configured but not yet built (e.g.
        still DRAFT-only, never reconstructed) is skipped with a
        warning rather than failing the whole run.
        """
        codes = self._distinct_source_codes()
        expected = {self._stg_tbl_name(code) for code in codes}
        existing = self._existing_tables()

        found = sorted(expected & existing)
        missing = sorted(expected - existing)
        if missing:
            print(
                f"WARNING: {len(missing)} expected stg_tbl not found and will be skipped: {missing}"
            )
        return found

    def _discover_stg_views(self) -> List[str]:
        """Same discovery logic as _discover_stg_tables(), but resolves
        each afs_source_code to its stg_{code}_vw name instead — the
        raw, per-source view that stg_{code}_tbl itself reads from,
        before any indicator-level filter_string is applied.
        """
        codes = self._distinct_source_codes()
        expected = {self._stg_view_name(code) for code in codes}
        existing = self._existing_tables()

        found = sorted(expected & existing)
        missing = sorted(expected - existing)
        if missing:
            print(
                f"WARNING: {len(missing)} expected stg_vw not found and will be skipped: {missing}"
            )
        return found

    def _check_schema_consistency(self, table_names: List[str], label: str = "stg_tbl") -> None:
        """UNION ALL matches columns positionally, so every table/view
        going into a union must have the exact same column names in the
        exact same order — a plain `SELECT *` union will produce
        silently wrong results (or a cryptic BigQuery error) otherwise.

        Fetches each object's column list (one get_table() call per
        object — metadata only, no query cost) and compares them all
        against whichever schema the majority share. Raises with a
        clear per-object diff naming exactly which one(s) are the odd
        one(s) out and how (missing columns, extra columns, or same
        columns in a different order), rather than letting the first
        confusing BigQuery UNION ALL error be the only clue.

        `label` is purely cosmetic (used in print/error text) so the
        same logic can serve both the stg_tbl union (build_long_view)
        and the stg_vw union (build_src_view) with messages that name
        the right kind of object. Called from validate() and from both
        build_long_view()/build_src_view(), so the problem surfaces
        whichever way the pipeline is run.
        """
        if not table_names:
            return

        schemas: Dict[str, tuple] = {
            t: tuple(f.name for f in self.bq_client.get_table(self._table_fqn(t)).schema)
            for t in table_names
        }

        counts = Counter(schemas.values())
        baseline_schema, n_agree = counts.most_common(1)[0]
        mismatches = {t: cols for t, cols in schemas.items() if cols != baseline_schema}
        if not mismatches:
            print(f"Schema check OK: all {len(table_names)} {label} share the same {len(baseline_schema)} columns.")
            return

        baseline_set = set(baseline_schema)
        lines = [
            f"Schema mismatch across {len(mismatches)}/{len(table_names)} {label}. "
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
    # Step 1 — long view (union of stg_*_tbl, processed)
    # ------------------------------------------------------------------

    def _build_long_view_query(self, stg_tables: List[str]) -> str:
        if not stg_tables:
            raise ValueError("No stg_tbl available to union into the long view.")
        selects = [f"SELECT * FROM `{self._table_fqn(t)}` WHERE afs_year >= {self.start_year}" for t in stg_tables]
        return "\nUNION ALL\n".join(selects)

    def build_long_view(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """CREATE OR REPLACE VIEW exp_stg_all_long_vw as the UNION ALL
        of every stg_tbl found by _discover_stg_tables() (indicator-
        filtered, i.e. "processed"). Always a full replace — a view
        definition, not stored data, so there's no incremental mode to
        speak of. rows_affected is left None here: a view holds no
        data of its own to count.

        Runs _check_schema_consistency() first (even on a dry run —
        it's metadata-only, no query cost) so a column mismatch across
        stg_tbl is caught here, with a per-table diff, instead of
        surfacing later as a confusing UNION ALL error from BigQuery.
        """
        stg_tables = self._discover_stg_tables()
        self._check_schema_consistency(stg_tables, label="stg_tbl")
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
    # Step 2 — src view (union of stg_*_vw, unprocessed)
    # ------------------------------------------------------------------

    def _build_src_view_query(self, stg_views: List[str]) -> str:
        if not stg_views:
            raise ValueError("No stg_vw available to union into the src view.")
        selects = [f"SELECT * FROM `{self._table_fqn(v)}` WHERE afs_year >= {self.start_year}" for v in stg_views]
        return "\nUNION ALL\n".join(selects)

    def build_src_view(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """CREATE OR REPLACE VIEW exp_src_all_vw as the UNION ALL of
        every stg_vw found by _discover_stg_views() — the raw,
        per-source views (i.e. "unprocessed": before any indicator-level
        filter_string is applied), as opposed to build_long_view()'s
        indicator-filtered stg_tbl union. Same shape as build_long_view()
        otherwise: always a full replace, schema-checked first, no
        rows_affected (view holds no data of its own).
        """
        stg_views = self._discover_stg_views()
        self._check_schema_consistency(stg_views, label="stg_vw")
        union_query = self._build_src_view_query(stg_views)

        if not confirm_apply:
            print(
                f"[DRY RUN] build_src_view(): would union {len(stg_views)} stg_vw "
                f"into `{self.src_view_name}`: {stg_views}. Set confirm_apply=True to execute."
            )
            return {"stg_views": stg_views, "sql": union_query}

        sql = f"CREATE OR REPLACE VIEW `{self._src_view_fqn}` AS\n{union_query}"
        result = self._run_job(sql)
        result["step"] = STEP_BUILD_SRC_VIEW
        result["object_name"] = self.src_view_name
        result["source_stg_tbls"] = stg_views
        return result

    # ------------------------------------------------------------------
    # Step 3 — scaffold table
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
        # SELECT
          scope.m49_code AS afs_m49_code,
          scope.area_name AS afs_area_name,
          years.year AS afs_year,
          indicators.*
        FROM `{self._scope_table_fqn}` AS scope
        CROSS JOIN years
        CROSS JOIN indicators
        ORDER BY afs_m49_code, afs_indicator_uid, afs_year
        """

    def build_scaffold_table(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """CREATE OR REPLACE TABLE exp_ref_scaffold_cyi_tbl: every
        m49_code x year x ACTIVE/REVIEW-indicator combination.
        Materialized on its own — this used to be an inline subquery
        inside build_final_table()'s FULL JOIN; pulling it out means it
        can be inspected directly and is computed once even though
        build_stg_all_table() consumes it every run. Always a full
        rebuild — no incremental mode. rows_affected comes from an
        extra get_table() call after success, same reasoning as
        StagePipeline: CREATE TABLE AS SELECT is DDL, so BigQuery
        doesn't populate num_dml_affected_rows for it.
        """
        scaffold_query = self._build_scaffold_query()

        if not confirm_apply:
            print(
                f"[DRY RUN] build_scaffold_table(): would (re)build `{self.scaffold_table_name}` "
                f"clustered by (afs_m49_code, afs_indicator_uid). Set confirm_apply=True to execute."
            )
            return {"sql": scaffold_query}

        sql = (
            f"CREATE OR REPLACE TABLE `{self._scaffold_table_fqn}`\n"
            f"\nAS\n{scaffold_query}"
        )
        result = self._run_job(sql, count_table=self.scaffold_table_name)
        result["step"] = STEP_BUILD_SCAFFOLD_TABLE
        result["object_name"] = self.scaffold_table_name
        return result

    # ------------------------------------------------------------------
    # Step 4 — stg_all table (scaffold FULL JOIN long view, gap-filled)
    # ------------------------------------------------------------------

    def _build_stg_all_table_query(self) -> str:
        """exp_ref_scaffold_cyi_tbl FULL JOIN exp_stg_all_long_vw. FULL
        (not LEFT) so a long-view row with no matching scaffold
        combination — wrong m49_code, a year outside [start_year,
        current_year], or an indicator no longer ACTIVE/REVIEW — still
        surfaces instead of being dropped. The three join keys are
        COALESCEd from both sides so a long-view-only row still carries
        a non-NULL sort/cluster key.
        """
        return f"""
        SELECT
          scaffold.afs_m49_code AS afs_m49_code,
          scaffold.afs_indicator_uid AS afs_indicator_uid,
          scaffold.afs_year AS afs_year,
          scaffold.* EXCEPT (afs_m49_code, afs_indicator_uid, afs_year),
          long_vw.* EXCEPT (afs_m49_code, afs_indicator_uid, afs_year)
        FROM `{self._scaffold_table_fqn}` AS scaffold
        FULL JOIN `{self._long_view_fqn}` AS long_vw
          ON scaffold.afs_m49_code = long_vw.afs_m49_code
          AND scaffold.afs_indicator_uid = long_vw.afs_indicator_uid
          AND scaffold.afs_year = long_vw.afs_year
        """

    def build_stg_all_table(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """CREATE OR REPLACE TABLE exp_stg_all_long_tbl, clustered by
        (afs_m49_code, afs_indicator_uid). Reads exp_ref_scaffold_cyi_tbl
        and exp_stg_all_long_vw, so build_scaffold_table() and
        build_long_view() must already have been run successfully.
        Always a full rebuild — no incremental mode. rows_affected
        comes from an extra get_table() call after success, same
        reasoning as StagePipeline: CREATE TABLE AS SELECT is DDL, so
        BigQuery doesn't populate num_dml_affected_rows for it.
        """
        final_query = self._build_stg_all_table_query()

        if not confirm_apply:
            print(
                f"[DRY RUN] build_stg_all_table(): would (re)build `{self.stg_all_table_name}` "
                f"clustered by (afs_m49_code, afs_indicator_uid). Set confirm_apply=True to execute."
            )
            return {"sql": final_query}

        sql = (
            f"CREATE OR REPLACE TABLE `{self._stg_all_table_fqn}`\n"
            f"CLUSTER BY afs_m49_code, afs_indicator_uid\nAS\n{final_query}"
        )
        result = self._run_job(sql, count_table=self.stg_all_table_name)
        result["step"] = STEP_BUILD_STG_ALL_TABLE
        result["object_name"] = self.stg_all_table_name
        return result

    # ------------------------------------------------------------------
    # Step 5 — src_all table (straight materialization, no scaffold)
    # ------------------------------------------------------------------

    def _build_src_all_table_query(self) -> str:
        return f"SELECT * FROM `{self._src_view_fqn}`"

    def build_src_all_table(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """CREATE OR REPLACE TABLE exp_src_all_tbl, clustered by
        (afs_m49_code, afs_indicator_uid) same as exp_stg_all_long_tbl.
        Unlike build_stg_all_table(), this is a straight materialization
        of exp_src_all_vw — no scaffold, no FULL JOIN, no gap-filling —
        so it always holds exactly what's physically present across
        every stg_vw, as a complete unfiltered reference. Reads
        exp_src_all_vw, so build_src_view() must already have been run
        successfully. Always a full rebuild — no incremental mode.
        """
        final_query = self._build_src_all_table_query()

        if not confirm_apply:
            print(
                f"[DRY RUN] build_src_all_table(): would (re)build `{self.src_table_name}` "
                f"clustered by (afs_m49_code, afs_indicator_uid). Set confirm_apply=True to execute."
            )
            return {"sql": final_query}

        sql = (
            f"CREATE OR REPLACE TABLE `{self._src_table_fqn}`\n"
            f"CLUSTER BY afs_m49_code, afs_indicator_uid\nAS\n{final_query}"
        )
        result = self._run_job(sql, count_table=self.src_table_name)
        result["step"] = STEP_BUILD_SRC_TABLE
        result["object_name"] = self.src_table_name
        return result

    # ------------------------------------------------------------------
    # Step 6 — export both tables to GCS
    # ------------------------------------------------------------------

    def _export_one_table_sql(self, table_fqn: str, uri: str) -> str:
        return f"""
        EXPORT DATA OPTIONS(
          uri = '{uri}',
          format = 'PARQUET',
          overwrite = true
        ) AS
        SELECT *
        FROM `{table_fqn}`
        ORDER BY afs_m49_code, afs_indicator_uid, afs_year
        """

    def export_to_gcs(self, confirm_apply: bool = False) -> List[Dict[str, Any]]:
        """EXPORT DATA for both exp_stg_all_long_tbl and exp_src_all_tbl,
        each to its own fixed path under gcs_base_uri
        (`{gcs_base_uri}/{table_name}.parquet`), single Parquet file,
        overwrite=true, so the same two GCS URLs are reused every run.
        Re-applies ORDER BY explicitly for both — CLUSTER BY controls
        physical storage/pruning inside BigQuery, it does not guarantee
        row order in query output or export files. rows_affected reuses
        each table's row count (via get_table, same as the build_*
        steps) since each export is an unfiltered SELECT * and can't
        change the row count.

        Returns a list of two result dicts (one per table) instead of
        a single dict, since this step now covers two independent
        EXPORT DATA jobs.
        """
        targets = [
            (self.stg_all_table_name, self._stg_all_table_fqn, self._stg_all_export_uri),
            (self.src_table_name, self._src_table_fqn, self._src_all_export_uri),
        ]

        if not confirm_apply:
            preview = {name: uri for name, _, uri in targets}
            print(
                f"[DRY RUN] export_to_gcs(): would export {preview} "
                f"(single file each, overwrite=True). Set confirm_apply=True to execute."
            )
            return [{"table_name": name, "gcs_uri": uri, "sql": self._export_one_table_sql(fqn, uri)}
                    for name, fqn, uri in targets]

        results = []
        for table_name, table_fqn, uri in targets:
            sql = self._export_one_table_sql(table_fqn, uri)
            result = self._run_job(sql, count_table=table_name)
            result["step"] = STEP_EXPORT_GCS
            result["object_name"] = uri
            results.append(result)
        return results

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
        steps are sequential, not independent units of work. Since
        export_to_gcs() now returns two results in one call, run_all()
        flattens them in before passing results here — this method
        itself doesn't need to know how many rows came from which
        step."""
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
        """Runs, in dependency order:

            build_long_view -> build_src_view -> build_scaffold_table
            -> build_stg_all_table -> build_src_all_table -> export_to_gcs

        Unlike StagePipeline's per-source-group independence, these
        steps are strictly sequential — each depends on one or more
        previous steps' output — so this stops at the first failure
        instead of continuing to the next step. Calls validate() first.

        Nothing is executed until confirm_apply=True; calling this with
        the default runs validate() and returns an empty preview
        (inspect each step's own dry run individually for SQL detail).
        """
        self.validate()

        if not confirm_apply:
            print(
                "[DRY RUN] run_all(): would run build_long_view -> build_src_view -> "
                "build_scaffold_table -> build_stg_all_table -> build_src_all_table -> "
                f"export_to_gcs() -> '{self.gcs_base_uri}'. Set confirm_apply=True to execute."
            )
            return pd.DataFrame()

        run_id = self._new_run_id()
        results: List[Dict[str, Any]] = []

        steps = [
            ("1/6", "build_long_view", self.build_long_view),
            ("2/6", "build_src_view", self.build_src_view),
            ("3/6", "build_scaffold_table", self.build_scaffold_table),
            ("4/6", "build_stg_all_table", self.build_stg_all_table),
            ("5/6", "build_src_all_table", self.build_src_all_table),
        ]

        for step_label, step_name, step_fn in steps:
            print(f"[{run_id}] STEP {step_label}: {step_name}...")
            r = step_fn(confirm_apply=True)
            results.append(r)
            print(f"  -> {r['job_status']}" + (f": {r['error_message']}" if r["job_status"] == "FAILED" else ""))
            if r["job_status"] != "SUCCESS":
                self._write_log(run_id, results)
                print(f"[{run_id}] Stopped after STEP {step_label} ({step_name}) failure.")
                return pd.DataFrame(results)

        print(f"[{run_id}] STEP 6/6: export_to_gcs...")
        export_results = self.export_to_gcs(confirm_apply=True)
        results.extend(export_results)
        for r in export_results:
            print(f"  -> {r['object_name']}: {r['job_status']}" + (f": {r['error_message']}" if r["job_status"] == "FAILED" else ""))

        self._write_log(run_id, results)
        return pd.DataFrame(results)

# -*- coding: utf-8 -*-
"""stage_pipeline.py

StagePipeline — the BigQuery-only workflow that builds/refreshes
staging tables (`stg_*`) from the indicator configuration table
(`cfg_indicator_tbl`).

This module only reads/writes BigQuery. It has no knowledge of the
Google Sheet side of the config lifecycle (`cfg_indicator_gstbl`,
`ConfigSheetSyncer`) — that sync is handled entirely by
`sync_stage_configuration` / `sync_source_config` and the shared
`ConfigSheetSyncer` class. This class only *consumes* `cfg_indicator_tbl`
once it has already been synced; it never creates or modifies that
table's structure.

Status lifecycle reminder (for context — only the DRAFT -> REVIEW step
below is actually performed by this class):

    DRAFT --(this class, update() / reconstruct())--> REVIEW --(human review in Sheet,
    then ConfigSheetSyncer.push())--> ACTIVE --(manual)--> INACTIVE
    --(manual)--> DELETED

Two entry points, matching two distinct operational flows:

- `update()`      — status DRAFT only. Writes each DRAFT afs_uid's data
                     into its `stg_tbl` (CREATE if the table doesn't
                     exist yet; otherwise DELETE that afs_uid's old rows
                     + INSERT in one transaction, so re-running is safe).
                     On success, promotes the affected DRAFT rows to
                     REVIEW.

- `reconstruct()` — status ACTIVE / REVIEW / DRAFT. Rebuilds one or all
                     `stg_tbl` from scratch (CREATE OR REPLACE) when an
                     upstream data source has changed. INACTIVE rows are
                     not included, so their data drops out of the rebuilt
                     table. On success, promotes the affected DRAFT rows
                     to REVIEW; ACTIVE / REVIEW rows are left as they are.

DRAFT -> REVIEW is the only status transition this class ever performs.

Execution granularity is one BigQuery job per (stg_view, stg_tbl) group.
`stg_view`/`stg_tbl` are not stored columns — they're derived from each
row's `afs_source_code` as `src_{afs_source_code}_vw` /
`stg_{afs_source_code}_tbl`, so a source view feeds exactly one `stg_tbl`
and doesn't affect any other group. (The key/column is still called
`stg_view` in the plan and log table for backward compatibility, but the
view it points to is the `src_*_vw` view.)

Each cfg row is one sub_indicator: `afs_uid` identifies the
sub_indicator, and every row written into a `stg_tbl` carries `afs_uid`
(taken from the cfg row). This is effectively "one job per
source": a failure in one group never blocks or rolls back another, and
every group's outcome (success or failure) is logged independently to
the log table, tagged with the list of `afs_uid` it covered so you can
trace any row back to the run that touched it.

All checks happen once, in `validate()`, which runs automatically when
the class is instantiated: every cfg row except INACTIVE / DELETED is
loaded and checked together (valid status, non-blank afs_uid /
afs_source_code, unique afs_uid, existing src_*_vw). Invalid rows are
printed and skipped; `update()` / `reconstruct()` only run the rows that
passed and never re-validate. Call `validate()` again (or create a new
instance) after re-syncing the config.

Nothing is executed until `confirm_apply=True` is passed explicitly;
calling `update()` / `reconstruct()` without it returns a preview
DataFrame of what *would* run, for review before committing.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError


WriteMode = Literal["delete_insert", "create", "create_or_replace"]

REQUIRED_CFG_COLUMNS = {
    "afs_uid", "afs_source_code", "filter_string", "status",
}

# cfg rows with these statuses are never loaded, so they're left out of
# every check (an INACTIVE row can't make a live row look like a duplicate).
IGNORED_STATUSES = ["INACTIVE", "DELETED"]

# Every loaded row must have one of these statuses to pass validation.
# update() runs the DRAFT subset; reconstruct() runs all of them.
RUNNABLE_STATUSES = ["DRAFT", "REVIEW", "ACTIVE"]

# Optional cfg column (not in REQUIRED_CFG_COLUMNS — the cfg table's
# structure is owned by sync_stage_configuration.py, and a missing column
# just means "no pre_process anywhere"). When non-blank it holds a full,
# standalone SQL query that reads from a table called `input` (the
# filter_string-filtered rows of the source view, with afs_uid prepended).
PRE_PROCESS_COLUMN = "pre_process"

# Cap stored error text so one long BigQuery error message can't blow up
# a log table row. Long enough to keep the useful part of the message;
# anything past this is truncated with a marker rather than dropped.
MAX_ERROR_MESSAGE_LEN = 2000


class StagePipeline:
    """The BigQuery-only pipeline that builds and refreshes stg_* tables
    from cfg_indicator_tbl.

    All reads/writes are scoped to `{project_id}.{dataset_id}`. This
    class only talks to BigQuery — no Sheets access, no knowledge of
    cfg_indicator_gstbl. It owns and can create its own log table, but
    it never creates or alters cfg_indicator_tbl — that table is owned
    by sync_stage_configuration.py.

    Instantiating the class runs validate() once: cfg_indicator_tbl is
    read and checked up front, and only the rows that pass are kept for
    update() / reconstruct().
    """

    def __init__(
        self,
        bq_client: bigquery.Client,
        project_id: str,
        dataset_id: str,
        config_table_name: str,
        log_table_name: str,
    ):
        self.bq_client = bq_client
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.config_table_name = config_table_name
        self.log_table_name = log_table_name
        self._view_columns_cache: Dict[str, List[str]] = {}
        self._cfg: pd.DataFrame = pd.DataFrame()
        self.validate()

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------

    @property
    def _config_table_fqn(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.config_table_name}"

    @property
    def _log_table_fqn(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.log_table_name}"

    def _table_fqn(self, table_name: str) -> str:
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    @staticmethod
    def _new_run_id() -> str:
        """Timestamp-based run id, e.g. '20260914-153045'. Sortable and
        human-readable, so two runs from the same manual session are
        easy to spot side by side in the log table."""
        return datetime.now().strftime("%Y%m%d-%H%M%S")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _format_error(exc: Exception) -> str:
        """Concise, storage-friendly error text: str(exc) truncated to
        MAX_ERROR_MESSAGE_LEN. We deliberately don't store a full
        traceback here — the BQ job_id in the same log row is enough to
        go pull full job details from the BigQuery console/API if
        needed, and keeping the log table lean matters more than having
        the traceback duplicated in two places."""
        msg = str(exc)
        if len(msg) > MAX_ERROR_MESSAGE_LEN:
            msg = msg[:MAX_ERROR_MESSAGE_LEN] + " …[truncated]"
        return msg

    def _existing_tables(self) -> set:
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        return {t.table_id for t in self.bq_client.list_tables(dataset_ref)}

    def _view_columns(self, view_name: str) -> List[str]:
        """Column names of a src_*_vw view, in schema order. Cached per
        view so a plan with many pre_process rows on the same source
        only fetches the schema once."""
        if view_name not in self._view_columns_cache:
            table = self.bq_client.get_table(self._table_fqn(view_name))
            self._view_columns_cache[view_name] = [f.name for f in table.schema]
        return self._view_columns_cache[view_name]

    def _dry_run_error(self, sql: str) -> Optional[str]:
        """BigQuery dry run (no cost, nothing written). Returns None if
        the SQL is valid, otherwise the formatted error text."""
        try:
            self.bq_client.query(
                sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            )
            return None
        except Exception as exc:  # noqa: BLE001 - any failure means "not valid"
            return self._format_error(exc)

    # ------------------------------------------------------------------
    # Validation — runs once at instantiation: confirms the cfg table
    # exists, creates the log table if needed, loads and checks cfg rows
    # ------------------------------------------------------------------

    @staticmethod
    def _log_table_schema() -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("run_type", "STRING"),        # 'UPDATE' / 'RECONSTRUCT'
            bigquery.SchemaField("stg_view", "STRING"),
            bigquery.SchemaField("stg_tbl", "STRING"),
            bigquery.SchemaField("afs_uid_list", "STRING", mode="REPEATED"),
            bigquery.SchemaField("job_status", "STRING"),      # 'SUCCESS' / 'FAILED'
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("bq_job_id", "STRING"),
            bigquery.SchemaField("rows_affected", "INTEGER"),
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

    def validate(self) -> None:
        """All pre-flight checks, in one place. __init__ calls this
        automatically; call it again yourself only after cfg_indicator_tbl
        has been re-synced (e.g. after editing the config Sheet), to
        reload and re-check it. update() / reconstruct() never call it.

        1. cfg_indicator_tbl must exist. This class never creates it —
           that table is owned by sync_stage_configuration.py, and a
           missing cfg table means something upstream hasn't run yet, so
           this raises rather than silently continuing.
        2. The log table is created with the schema above if it doesn't
           exist yet.
        3. All cfg rows except INACTIVE / DELETED are loaded and checked
           together (see _drop_invalid_rows), so uniqueness holds across
           DRAFT, REVIEW and ACTIVE at once. Invalid rows are printed and
           skipped; the rest are cached, and every cached row is runnable.
        """
        existing = self._existing_tables()

        if self.config_table_name not in existing:
            raise RuntimeError(
                f"Config table '{self.config_table_name}' not found in "
                f"{self.project_id}.{self.dataset_id}. This table is expected to "
                f"already exist (owned by sync_stage_configuration.py) — "
                f"StagePipeline does not create it."
            )
        print(f"Config table '{self.config_table_name}' found.")

        if self.log_table_name not in existing:
            print(f"Log table '{self.log_table_name}' not found — creating it.")
            self._create_log_table()
        else:
            print(f"Log table '{self.log_table_name}' found.")

        df_cfg = self._load_cfg()
        n_loaded = len(df_cfg)
        self._cfg = self._drop_invalid_rows(df_cfg, existing) if n_loaded else df_cfg

        by_status = self._cfg["status"].value_counts().to_dict() if not self._cfg.empty else {}
        counts = ", ".join(f"{s}: {by_status.get(s, 0)}" for s in RUNNABLE_STATUSES)
        print(f"Validated cfg: {len(self._cfg)} runnable row(s) ({counts}), "
              f"{n_loaded - len(self._cfg)} skipped.")

    # ------------------------------------------------------------------
    # Step 1 — load cfg_indicator_tbl (called by validate() only)
    # ------------------------------------------------------------------

    def _load_cfg(self) -> pd.DataFrame:
        """Read every cfg_indicator_tbl row except INACTIVE / DELETED.
        A NULL status is loaded too, so _drop_invalid_rows can report it
        instead of it disappearing silently. filter_string is expected to
        already be a valid SQL WHERE-clause fragment (e.g.
        "col = 'a' AND other > 5"), or blank/NULL for "no filter", so it's
        used as-is when building the WHERE clause — no parsing here.
        """
        query = f"""
        SELECT *
        FROM `{self._config_table_fqn}`
        WHERE status IS NULL OR status NOT IN UNNEST(@ignored_statuses)
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter("ignored_statuses", "STRING", IGNORED_STATUSES)
        ])
        df_cfg = self.bq_client.query(query, job_config=job_config).to_dataframe()

        missing = REQUIRED_CFG_COLUMNS - set(df_cfg.columns)
        if missing:
            raise ValueError(f"cfg_indicator_tbl is missing required columns: {missing}")

        return df_cfg

    def _runnable_cfg(self, statuses: List[str], afs_source_code: Optional[str] = None) -> pd.DataFrame:
        """Slice of the rows validate() cached, by status (and optionally
        a single afs_source_code). No BigQuery read, no re-validation."""
        if self._cfg.empty:
            return self._cfg
        mask = self._cfg["status"].isin(statuses)
        if afs_source_code:
            mask &= self._cfg["afs_source_code"] == afs_source_code
        return self._cfg.loc[mask].reset_index(drop=True)

    @staticmethod
    def _is_blank(val: Any) -> bool:
        """True for None / NaN / NA / empty or whitespace-only strings."""
        if val is None:
            return True
        try:
            if pd.isna(val):
                return True
        except (TypeError, ValueError):
            pass
        return isinstance(val, str) and not val.strip()

    def _drop_invalid_rows(self, df_cfg: pd.DataFrame, existing_tables: set) -> pd.DataFrame:
        """Skip cfg rows that can't be processed safely. All checks run
        here, up front (from validate()), so a bad row is dropped on its
        own instead of raising mid-plan and blocking every group:

        - status not DRAFT / REVIEW / ACTIVE (incl. NULL): no entry point
          would ever run it.
        - blank afs_uid: the uid is what's written into the stg_tbl and
          what DRAFT -> REVIEW write-back matches on, so a blank one can't
          be traced or promoted (and NaN uids break sorting/grouping).
        - blank afs_source_code: no src_*_vw / stg_*_tbl can be derived.
        - duplicate afs_uid: each afs_uid must be exactly one cfg row
          (one sub_indicator, one filter_string / pre_process), across all
          loaded statuses. Every row of a duplicated afs_uid is skipped,
          since there's no safe way to pick one.
        - missing src_*_vw: the source view for afs_source_code doesn't
          exist in the dataset.

        Skipped rows are printed so they can be fixed and re-synced; they
        are never modified here (a skipped DRAFT row stays DRAFT and will
        be picked up once it's fixed).
        """
        bad_status = ~df_cfg["status"].isin(RUNNABLE_STATUSES)
        no_uid = df_cfg["afs_uid"].map(self._is_blank)
        no_src = df_cfg["afs_source_code"].map(self._is_blank)
        dup_uid = df_cfg["afs_uid"].duplicated(keep=False) & ~no_uid
        no_view = ~no_src & ~df_cfg["afs_source_code"].map(
            lambda code: self._stg_view_name(code) in existing_tables
        )

        checks = [
            (f"status not in ({', '.join(RUNNABLE_STATUSES)})", bad_status),
            ("blank afs_uid", no_uid),
            ("blank afs_source_code", no_src),
            ("duplicate afs_uid", dup_uid),
            ("missing src_{afs_source_code}_vw", no_view),
        ]
        show_cols = ["afs_uid", "afs_source_code", "status", "filter_string"]
        for reason, mask in checks:
            if mask.any():
                print(f"WARNING: skipping {int(mask.sum())} cfg row(s) with {reason}. "
                      f"Fix them and re-sync to include them:")
                print(df_cfg.loc[mask, show_cols].to_string())

        skip = bad_status | no_uid | no_src | dup_uid | no_view
        return df_cfg.loc[~skip].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Step 2 — build the SQL plan (pure, no BQ writes)
    # ------------------------------------------------------------------

    @staticmethod
    def _format_value(val: Any) -> str:
        """Formats a scalar Python value into a valid SQL string literal
        or primitive. Used for values this class constructs itself (e.g.
        afs_uid), not for filter_string, which is already a ready-to-use
        SQL fragment supplied by the config.

        Escaping follows GoogleSQL (BigQuery) string-literal rules: quotes
        are escaped with a backslash (\\'), not doubled ('') — BigQuery
        reads 'a''b' as two adjacent literals and rejects it. Backslashes
        are escaped first so they aren't read as escape sequences, and raw
        newlines are escaped because a '...' literal can't span lines."""
        if val is None or pd.isna(val):
            return "NULL"
        if isinstance(val, str):
            escaped = (
                val.replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
            )
            return f"'{escaped}'"
        return str(val)

    @staticmethod
    def _clean_pre_process(val: Any) -> str:
        """Blank/NULL -> ''. Otherwise strip whitespace and any trailing
        ';' so the query can be nested inside a subquery."""
        if val is None or pd.isna(val):
            return ""
        return str(val).strip().rstrip(";").strip()

    @staticmethod
    def _stg_view_name(afs_source_code: str) -> str:
        return f"src_{afs_source_code}_vw"

    @staticmethod
    def _stg_tbl_name(afs_source_code: str) -> str:
        return f"stg_{afs_source_code}_tbl"

    def _build_indicator_queries(self, df_cfg: pd.DataFrame) -> pd.DataFrame:
        """One row per cfg row: the per-sub_indicator SELECT that will
        later be UNION ALL'd together with the other sub_indicators
        feeding the same stg_tbl. afs_uid identifies the sub_indicator and
        is written to the stg_tbl as the afs_uid column.

        Each cfg row is one complete sub_indicator — afs_uid is unique and
        its src_*_vw exists, both already enforced by _drop_invalid_rows. filter_string
        holds the full WHERE-clause fragment for that row (blank/NULL
        means "no filter", i.e. select everything).

        pre_process (optional column): when non-blank, the filtered
        SELECT above becomes a CTE named `input`, and the pre_process
        query (a full standalone query, may start with its own WITH)
        runs on top of it. Its output is re-selected by explicit column
        name — afs_uid followed by the src view's columns in schema
        order — so it always lines up with the other sub_indicators in
        the UNION ALL and with the stg_tbl, and a missing column fails
        with a clear "Unrecognized name" error instead of shifting
        columns silently.
        """
        empty_cols = [
            "afs_uid", "afs_source_code", "stg_view", "stg_tbl",
            "indicator_src_query", "has_pre_process",
        ]
        if df_cfg.empty:
            return pd.DataFrame(columns=empty_cols)

        results = []

        for rec in df_cfg.to_dict("records"):
            uid, afs_source_code = rec["afs_uid"], rec["afs_source_code"]
            filter_string = rec["filter_string"]
            condition = "" if self._is_blank(filter_string) else str(filter_string).strip()
            pre_process = self._clean_pre_process(rec.get(PRE_PROCESS_COLUMN))

            stg_view = self._stg_view_name(afs_source_code)
            stg_tbl = self._stg_tbl_name(afs_source_code)

            formatted_uid = self._format_value(uid)
            select_clause = f"{formatted_uid} AS afs_uid, *"

            if not pre_process:
                where_clause = f"\n    WHERE {condition}" if condition else ""
                sql_query = f"""(
    SELECT {select_clause}
    FROM `{self._table_fqn(stg_view)}`{where_clause}
)"""
            else:
                where_clause = f"\n        WHERE {condition}" if condition else ""
                columns = ",\n        ".join(
                    f"`{c}`" for c in ["afs_uid"] + self._view_columns(stg_view)
                )
                sql_query = f"""(
    WITH input AS (
        SELECT {select_clause}
        FROM `{self._table_fqn(stg_view)}`{where_clause}
    )
    SELECT
        {columns}
    FROM (
{pre_process}
    )
)"""
            results.append({
                "afs_uid": uid,
                "afs_source_code": afs_source_code,
                "stg_view": stg_view,
                "stg_tbl": stg_tbl,
                "indicator_src_query": sql_query,
                "has_pre_process": bool(pre_process),
            })

        return pd.DataFrame(results, columns=empty_cols) if not results else pd.DataFrame(results)

    def _build_query_plan(self, df_cfg: pd.DataFrame) -> pd.DataFrame:
        """Group per-indicator queries into one UNION ALL query per
        (stg_view, stg_tbl) — this is the execution granularity for the
        whole pipeline. Carries the list of afs_uid each group covers,
        for status write-back and logging.
        """
        empty_cols = ["stg_view", "stg_tbl", "indicator_src_query", "afs_uid_list", "has_pre_process"]
        df_indicator_queries = self._build_indicator_queries(df_cfg)
        if df_indicator_queries.empty:
            return pd.DataFrame(columns=empty_cols)

        plan = (
            df_indicator_queries.groupby(["stg_view", "stg_tbl"])
            .agg(
                indicator_src_query=("indicator_src_query", " UNION ALL ".join),
                afs_uid_list=("afs_uid", lambda s: sorted(set(s))),
                has_pre_process=("has_pre_process", "any"),
            )
            .reset_index()
        )
        return plan

    def _preview(self, plan: pd.DataFrame) -> pd.DataFrame:
        """Dry-run preview: dry-runs each group's SELECT in BigQuery (no
        cost, nothing written) so SQL errors — typically in a
        pre_process cell — show up before confirm_apply=True."""
        preview = plan.copy()
        preview["dry_run_error"] = preview["indicator_src_query"].apply(self._dry_run_error)
        n_failed = int(preview["dry_run_error"].notna().sum())
        if n_failed:
            print(f"WARNING: {n_failed} group(s) failed the BigQuery dry run — see dry_run_error.")
        return preview[[
            "stg_view", "stg_tbl", "write_mode", "afs_uid_list",
            "has_pre_process", "dry_run_error", "indicator_src_query",
        ]]

    # ------------------------------------------------------------------
    # Step 3 — execute one (stg_view, stg_tbl) group
    # ------------------------------------------------------------------

    def _execute_group(
        self,
        stg_tbl: str,
        src_query: str,
        write_mode: WriteMode,
        afs_uid_list: List[Any],
    ) -> Dict[str, Any]:
        """Run the BQ job for a single stg_tbl group. Never raises —
        failures are captured and returned so the caller can keep going
        with the remaining groups in the plan.

        'delete_insert' deletes the group's afs_uid from the stg_tbl and
        inserts them again, inside one transaction (rolled back on any
        error, so the table is never left half-written). Running it twice
        for the same afs_uid gives the same result as running it once —
        e.g. a row that was put back to DRAFT, or whose status write-back
        failed, is rewritten rather than duplicated.

        rows_affected: for 'delete_insert' it's the INSERT statement's
        num_dml_affected_rows, read from the script's child job. For
        'create' / 'create_or_replace' that field is always None — CREATE
        TABLE AS SELECT is DDL, not DML, so BigQuery doesn't populate it —
        so we make one extra get_table() call after the job succeeds and
        use the table's current num_rows instead.
        """
        full_table = f"`{self._table_fqn(stg_tbl)}`"

        if write_mode == "delete_insert":
            uids = ", ".join(self._format_value(u) for u in afs_uid_list)
            sql = f"""BEGIN
  BEGIN TRANSACTION;
  DELETE FROM {full_table} WHERE afs_uid IN ({uids});
  INSERT INTO {full_table}
{src_query};
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = @@error.message;
END;"""
        elif write_mode == "create":
            sql = f"CREATE TABLE {full_table}\nAS\n{src_query}"
        elif write_mode == "create_or_replace":
            sql = f"CREATE OR REPLACE TABLE {full_table}\nAS\n{src_query}"
        else:
            raise ValueError(f"Unknown write_mode: {write_mode}")

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

            if write_mode == "delete_insert":
                for child in self.bq_client.list_jobs(parent_job=job.job_id):
                    if getattr(child, "statement_type", None) == "INSERT":
                        result["rows_affected"] = child.num_dml_affected_rows
            else:
                table = self.bq_client.get_table(self._table_fqn(stg_tbl))
                result["rows_affected"] = table.num_rows
        except (GoogleAPIError, Exception) as exc:  # noqa: BLE001 - broad on purpose, see docstring
            if job is not None:
                result["bq_job_id"] = job.job_id
            result["error_message"] = self._format_error(exc)

        return result

    # ------------------------------------------------------------------
    # Step 4 — logging (success and failure, always)
    # ------------------------------------------------------------------

    def _write_log(self, run_id: str, run_type: str, results: List[Dict[str, Any]]) -> None:
        """Write one log table row per group, success or failure, so the
        table doubles as a full execution history, not just an error
        log."""
        if not results:
            return

        now = self._now_iso()
        rows = [
            {
                "run_id": run_id,
                "run_type": run_type,
                "stg_view": r["stg_view"],
                "stg_tbl": r["stg_tbl"],
                "afs_uid_list": r.get("afs_uid_list", []),
                "job_status": r["job_status"],
                "error_message": r.get("error_message"),
                "bq_job_id": r.get("bq_job_id"),
                "rows_affected": r.get("rows_affected"),
                "created_at": now,
            }
            for r in results
        ]

        errors = self.bq_client.insert_rows_json(self._log_table_fqn, rows)
        if errors:
            # A logging failure shouldn't take down the pipeline run
            # itself (the stg_tbl writes already happened), but it must
            # not fail silently either.
            print(f"WARNING: failed to write {len(errors)} row(s) to {self.log_table_name}: {errors}")

    # ------------------------------------------------------------------
    # Step 5 — status write-back (UPDATE and RECONSTRUCT flows)
    # ------------------------------------------------------------------

    def _mark_reviewed(self, successful_uids: List[str]) -> int:
        """DRAFT -> REVIEW for the afs_uid that succeeded this run.
        Anything not in this list (including uids from failed groups) is
        left as DRAFT and retried the next time update() / reconstruct()
        runs.

        The validated cfg cache is updated first, then cfg_indicator_tbl
        only for the uids that were actually DRAFT in the cache (ACTIVE /
        REVIEW uids in the list are ignored, and no UPDATE runs if none
        were DRAFT).

        A failed BigQuery UPDATE is printed, not raised: the stg_tbl
        writes already happened, and since update() replaces each
        afs_uid's rows instead of appending, a later run that sees them as
        DRAFT again just rewrites the same data. Returns the number of cfg
        rows promoted in BigQuery (0 if the UPDATE failed)."""
        if self._cfg.empty or not successful_uids:
            return 0

        promoted = (self._cfg["status"] == "DRAFT") & self._cfg["afs_uid"].isin(successful_uids)
        uids = self._cfg.loc[promoted, "afs_uid"].tolist()
        if not uids:
            return 0
        self._cfg.loc[promoted, "status"] = "REVIEW"

        sql = f"""
        UPDATE `{self._config_table_fqn}`
        SET status = 'REVIEW'
        WHERE status = 'DRAFT' AND afs_uid IN UNNEST(@uids)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("uids", "STRING", uids)]
        )
        try:
            job = self.bq_client.query(sql, job_config=job_config)
            job.result()
        except Exception as exc:  # noqa: BLE001 - data is already written, don't fail the run
            print(f"WARNING: {len(uids)} afs_uid marked REVIEW in the local cfg cache, but the "
                  f"UPDATE on {self.config_table_name} failed, so they are still DRAFT in "
                  f"BigQuery (safe to re-run later): {self._format_error(exc)}")
            return 0
        return job.num_dml_affected_rows or 0

    # ------------------------------------------------------------------
    # Public entry point — UPDATE
    # ------------------------------------------------------------------

    def update(self, confirm_apply: bool = False) -> pd.DataFrame:
        """DRAFT rows only. For an existing stg_tbl, replaces each DRAFT
        afs_uid's rows (DELETE + INSERT in one transaction, see
        _execute_group); creates the stg_tbl (CREATE TABLE AS) if it
        doesn't exist yet. Safe to re-run: nothing is ever duplicated.

        On success, promotes the affected afs_uid from DRAFT to REVIEW;
        DRAFT rows in failed groups stay DRAFT and are retried next run.

        Runs on the rows validate() cached at instantiation; nothing is
        re-validated here. Nothing is executed until confirm_apply=True;
        calling this with the default returns a preview of the groups
        that would run.
        """
        df_cfg = self._runnable_cfg(["DRAFT"])
        if df_cfg.empty:
            print("No validated DRAFT rows. Nothing to update.")
            return pd.DataFrame()

        plan = self._build_query_plan(df_cfg)
        if plan.empty:
            print("No valid (stg_view, stg_tbl) groups could be built from the DRAFT rows.")
            return pd.DataFrame()

        existing_tables = self._existing_tables()
        plan = plan.copy()
        plan["write_mode"] = plan["stg_tbl"].apply(
            lambda t: "delete_insert" if t in existing_tables else "create"
        )

        if not confirm_apply:
            print(f"[DRY RUN] update(): {len(plan)} stg_tbl group(s) would be processed. "
                  f"Set confirm_apply=True to execute.")
            return self._preview(plan)

        run_id = self._new_run_id()
        results: List[Dict[str, Any]] = []
        for row in plan.itertuples(index=False):
            print(f"[{run_id}] UPDATE {row.stg_view} -> {row.stg_tbl} ({row.write_mode})...")
            res = self._execute_group(row.stg_tbl, row.indicator_src_query, row.write_mode, row.afs_uid_list)
            res["stg_view"] = row.stg_view
            res["stg_tbl"] = row.stg_tbl
            res["afs_uid_list"] = row.afs_uid_list
            results.append(res)
            status = res["job_status"]
            print(f"  -> {status}" + (f": {res['error_message']}" if status == "FAILED" else ""))

        self._write_log(run_id, "UPDATE", results)

        successful_uids = [
            uid
            for r in results if r["job_status"] == "SUCCESS"
            for uid in r["afs_uid_list"]
        ]
        n_updated = self._mark_reviewed(successful_uids)
        print(f"[{run_id}] Marked {n_updated} row(s) DRAFT -> REVIEW.")

        return pd.DataFrame(results)

    # ------------------------------------------------------------------
    # Public entry point — RECONSTRUCT
    # ------------------------------------------------------------------

    def reconstruct(
        self,
        afs_source_code: Optional[str] = None,
        confirm_apply: bool = False,
    ) -> pd.DataFrame:
        """ACTIVE / REVIEW / DRAFT rows only. Always CREATE OR REPLACE —
        the full stg_tbl is rebuilt from current source data, no
        appending. INACTIVE rows are excluded, so their data is dropped
        from the rebuilt stg_tbl.

        On success, the DRAFT afs_uid in each successfully rebuilt group
        are promoted to REVIEW. ACTIVE / REVIEW rows keep their status,
        and DRAFT rows in failed groups stay DRAFT.

        afs_source_code=None rebuilds every (stg_view, stg_tbl) group
        found; pass a specific afs_source_code to rebuild only the
        stg_tbl it feeds, without affecting any other stg_tbl.

        Runs on the rows validate() cached at instantiation; nothing is
        re-validated here. Nothing is executed until confirm_apply=True;
        calling this with the default returns a preview of the groups
        that would run.
        """
        df_cfg = self._runnable_cfg(RUNNABLE_STATUSES, afs_source_code=afs_source_code)
        if df_cfg.empty:
            scope = f" for afs_source_code='{afs_source_code}'" if afs_source_code else ""
            print(f"No validated cfg rows{scope}. Nothing to reconstruct.")
            return pd.DataFrame()

        plan = self._build_query_plan(df_cfg)
        if plan.empty:
            print("No valid (stg_view, stg_tbl) groups could be built from the selected rows.")
            return pd.DataFrame()

        if not confirm_apply:
            preview = plan.copy()
            preview["write_mode"] = "create_or_replace"
            print(f"[DRY RUN] reconstruct(): {len(preview)} stg_tbl group(s) would be rebuilt. "
                  f"Set confirm_apply=True to execute.")
            return self._preview(preview)

        run_id = self._new_run_id()
        results: List[Dict[str, Any]] = []
        for row in plan.itertuples(index=False):
            print(f"[{run_id}] RECONSTRUCT {row.stg_view} -> {row.stg_tbl} (create_or_replace)...")
            res = self._execute_group(row.stg_tbl, row.indicator_src_query, "create_or_replace", row.afs_uid_list)
            res["stg_view"] = row.stg_view
            res["stg_tbl"] = row.stg_tbl
            res["afs_uid_list"] = row.afs_uid_list
            results.append(res)
            status = res["job_status"]
            print(f"  -> {status}" + (f": {res['error_message']}" if status == "FAILED" else ""))

        self._write_log(run_id, "RECONSTRUCT", results)

        successful_uids = [
            uid
            for r in results if r["job_status"] == "SUCCESS"
            for uid in r["afs_uid_list"]
        ]
        n_updated = self._mark_reviewed(successful_uids)
        print(f"[{run_id}] Marked {n_updated} row(s) DRAFT -> REVIEW.")

        return pd.DataFrame(results)

    # ------------------------------------------------------------------
    # Public helper — stg_tbl no longer fed by the cfg
    # ------------------------------------------------------------------

    def find_unused_stg_tables(self) -> pd.DataFrame:
        """Read-only. Lists stg_{afs_source_code}_tbl tables that exist in
        the dataset but that no validated cfg row (DRAFT / REVIEW /
        ACTIVE) feeds — e.g. every row of that source is now INACTIVE /
        DELETED, or was skipped by validate(). update() / reconstruct()
        never touch these tables, so their data stays as of their last
        write. Nothing is dropped; this only reports.

        Based on the cfg cache from the last validate().
        """
        used = set(self._cfg["afs_source_code"].map(self._stg_tbl_name)) if not self._cfg.empty else set()
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)

        rows = []
        for item in self.bq_client.list_tables(dataset_ref):
            name = item.table_id
            if (item.table_type != "TABLE"
                    or not (name.startswith("stg_") and name.endswith("_tbl"))
                    or name in used
                    or name in (self.config_table_name, self.log_table_name)):
                continue
            table = self.bq_client.get_table(self._table_fqn(name))
            rows.append({
                "stg_tbl": name,
                "afs_source_code": name[len("stg_"):-len("_tbl")],
                "num_rows": table.num_rows,
                "modified": table.modified,
            })

        df = pd.DataFrame(rows, columns=["stg_tbl", "afs_source_code", "num_rows", "modified"])
        df = df.sort_values("stg_tbl").reset_index(drop=True)
        print(f"Found {len(df)} stg_tbl not fed by any validated cfg row.")
        return df

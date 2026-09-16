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

    DRAFT --(this class, update())--> REVIEW --(human review in Sheet,
    then ConfigSheetSyncer.push())--> ACTIVE --(manual)--> INACTIVE
    --(manual)--> DELETED

Two entry points, matching two distinct operational flows:

- `update()`      — status DRAFT only. Adds new data on top of what's
                     already in each `stg_tbl` (CREATE if the table
                     doesn't exist yet, INSERT INTO if it does). On
                     success, promotes the affected DRAFT rows to
                     REVIEW. This is the only status transition this
                     class ever performs, and it only ever runs
                     DRAFT -> REVIEW.

- `reconstruct()` — status ACTIVE / REVIEW / INACTIVE. Rebuilds one or
                     all `stg_tbl` from scratch (CREATE OR REPLACE) when
                     an upstream data source has changed. Never touches
                     status.

Execution granularity is one BigQuery job per (stg_view, stg_tbl) group.
A `stg_view` feeds exactly one `stg_tbl` and doesn't affect any other
group, so this is effectively "one job per source": a failure in one
group never blocks or rolls back another, and every group's outcome
(success or failure) is logged independently to the log table, tagged
with the list of `afs_uid` it covered so you can trace any row back to
the run that touched it.

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


WriteMode = Literal["insert", "create", "create_or_replace"]

REQUIRED_CFG_COLUMNS = {
    "filter_key", "filter_opt", "filter_value",
    "afs_uid", "src_idx", "stg_view", "stg_tbl", "status",
}

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

    # ------------------------------------------------------------------
    # Validation — confirms cfg table exists, creates the log table if
    # it doesn't exist yet
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
        """Confirms cfg_indicator_tbl exists (this class never creates
        it — that table is owned by sync_stage_configuration.py, and a
        missing cfg table means something upstream hasn't run yet, so
        this raises rather than silently continuing).

        Ensures the log table exists, creating it with the schema above
        if it doesn't. Safe to call repeatedly.

        update() and reconstruct() both call this automatically before
        doing anything else, so you don't strictly have to call it
        yourself — but calling it explicitly first lets you see the
        checks (and any table creation) happen before anything touches
        a stg_tbl.
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

    # ------------------------------------------------------------------
    # Step 1 — load cfg_indicator_tbl
    # ------------------------------------------------------------------

    def _load_cfg(
        self,
        statuses: List[str],
        stg_view: Optional[str] = None,
    ) -> pd.DataFrame:
        """Read cfg_indicator_tbl filtered by status (and optionally a
        single stg_view). filter_value is expected to already be a valid
        SQL fragment (e.g. "('a', 'b')" or "5"), so it's used as-is when
        building the WHERE clause — no parsing here.
        """
        query = f"""
        SELECT *
        FROM `{self._config_table_fqn}`
        WHERE status IN UNNEST(@statuses)
        {"AND stg_view = @stg_view" if stg_view else ""}
        """
        params: List[Any] = [bigquery.ArrayQueryParameter("statuses", "STRING", statuses)]
        if stg_view:
            params.append(bigquery.ScalarQueryParameter("stg_view", "STRING", stg_view))

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        df_cfg = self.bq_client.query(query, job_config=job_config).to_dataframe()

        if df_cfg.empty:
            return df_cfg

        missing = REQUIRED_CFG_COLUMNS - set(df_cfg.columns)
        if missing:
            raise ValueError(f"cfg_indicator_tbl is missing required columns: {missing}")

        return df_cfg

    # ------------------------------------------------------------------
    # Step 2 — build the SQL plan (pure, no BQ writes)
    # ------------------------------------------------------------------

    @staticmethod
    def _format_value(val: Any) -> str:
        """Formats a scalar Python value into a valid SQL string literal
        or primitive. Used for values this class constructs itself (e.g.
        afs_uid), not for filter_value, which is already a ready-to-use
        SQL fragment supplied by the config."""
        if val is None or pd.isna(val):
            return "NULL"
        if isinstance(val, str):
            escaped = val.replace("'", "''")
            return f"'{escaped}'"
        return str(val)

    @staticmethod
    def _build_condition(key: str, opt: str, val: Any) -> str:
        """Constructs a single SQL WHERE clause condition. val is taken
        as-is: filter_value is expected to already be a valid SQL
        fragment (e.g. "('a', 'b')" for an IN clause, or "5")."""
        return f"{key} {opt} {val}"

    def _build_indicator_queries(self, df_cfg: pd.DataFrame) -> pd.DataFrame:
        """One row per (afs_uid, src_idx, stg_view, stg_tbl) group: the
        per-indicator SELECT that will later be UNION ALL'd together
        with the other indicators feeding the same stg_tbl.
        """
        empty_cols = ["afs_uid", "src_idx", "stg_view", "stg_tbl", "indicator_src_query"]
        if df_cfg.empty:
            return pd.DataFrame(columns=empty_cols)

        df = df_cfg.copy()
        df["single_condition"] = df.apply(
            lambda row: self._build_condition(row["filter_key"], row["filter_opt"], row["filter_value"]),
            axis=1,
        )

        results = []
        group_cols = ["afs_uid", "src_idx", "stg_view", "stg_tbl"]

        for (uid, src_idx, stg_view, stg_tbl), group in df.groupby(group_cols, dropna=False):
            conditions = group["single_condition"].tolist()
            if not conditions:
                continue

            formatted_uid = self._format_value(uid)
            select_clause = f"{formatted_uid} AS afs_indicator_uid, *"
            where_clause = " AND ".join(conditions)

            sql_query = f"""(
    SELECT {select_clause}
    FROM `{self._table_fqn(stg_view)}`
    WHERE {where_clause}
)"""
            results.append({
                "afs_uid": uid,
                "src_idx": src_idx,
                "stg_view": stg_view,
                "stg_tbl": stg_tbl,
                "indicator_src_query": sql_query,
            })

        return pd.DataFrame(results, columns=empty_cols) if not results else pd.DataFrame(results)

    def _build_query_plan(self, df_cfg: pd.DataFrame) -> pd.DataFrame:
        """Group per-indicator queries into one UNION ALL query per
        (stg_view, stg_tbl) — this is the execution granularity for the
        whole pipeline. Carries the list of afs_uid each group covers,
        for status write-back and logging.
        """
        empty_cols = ["stg_view", "stg_tbl", "indicator_src_query", "afs_uid_list"]
        df_indicator_queries = self._build_indicator_queries(df_cfg)
        if df_indicator_queries.empty:
            return pd.DataFrame(columns=empty_cols)

        plan = (
            df_indicator_queries.groupby(["stg_view", "stg_tbl"])
            .agg(
                indicator_src_query=("indicator_src_query", " UNION ALL ".join),
                afs_uid_list=("afs_uid", lambda s: sorted(set(s))),
            )
            .reset_index()
        )
        return plan

    # ------------------------------------------------------------------
    # Step 3 — execute one (stg_view, stg_tbl) group
    # ------------------------------------------------------------------

    def _execute_group(
        self,
        stg_tbl: str,
        src_query: str,
        write_mode: WriteMode,
    ) -> Dict[str, Any]:
        """Run the BQ job for a single stg_tbl group. Never raises —
        failures are captured and returned so the caller can keep going
        with the remaining groups in the plan.

        rows_affected: for 'insert' this comes straight from the job's
        num_dml_affected_rows (INSERT INTO is DML). For 'create' /
        'create_or_replace' that field is always None — CREATE TABLE AS
        SELECT is DDL, not DML, so BigQuery doesn't populate it — so we
        make one extra get_table() call after the job succeeds and use
        the table's current num_rows instead. Either way rows_affected
        ends up with a real number rather than being blank for every
        reconstruct() run.
        """
        full_table = f"`{self._table_fqn(stg_tbl)}`"

        if write_mode == "insert":
            sql = f"INSERT INTO {full_table}\n{src_query}"
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

            if write_mode == "insert":
                result["rows_affected"] = job.num_dml_affected_rows
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
    # Step 5 — status write-back (UPDATE flow only)
    # ------------------------------------------------------------------

    def _mark_reviewed(self, successful_uids: List[str]) -> int:
        """DRAFT -> REVIEW, scoped to exactly the afs_uid that succeeded
        this run. Anything not in this list (including uids from failed
        groups) is left as DRAFT and will be retried automatically the
        next time update() runs."""
        if not successful_uids:
            return 0

        sql = f"""
        UPDATE `{self._config_table_fqn}`
        SET
            status = 'REVIEW',
            updated_at = FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S %Z', CURRENT_TIMESTAMP())
        WHERE status = 'DRAFT' AND afs_uid IN UNNEST(@uids)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("uids", "STRING", successful_uids)]
        )
        job = self.bq_client.query(sql, job_config=job_config)
        job.result()
        return job.num_dml_affected_rows or 0

    # ------------------------------------------------------------------
    # Public entry point — UPDATE
    # ------------------------------------------------------------------

    def update(
        self,
        confirm_apply: bool = False,
        confirm_status_update: bool = False,
    ) -> pd.DataFrame:
        """DRAFT rows only. Appends to existing stg_tbl (INSERT INTO) or
        creates it (CREATE TABLE AS) if it doesn't exist yet.

        On success, promotes the affected afs_uid from DRAFT to REVIEW —
        unless confirm_status_update is False, in which case data is
        written but status is left untouched.

        Nothing is executed until confirm_apply=True; calling this with
        the default returns a preview of the groups that would run.
        """
        self.validate()

        df_cfg = self._load_cfg(statuses=["DRAFT"])
        if df_cfg.empty:
            print("No DRAFT rows found in cfg_indicator_tbl. Nothing to update.")
            return pd.DataFrame()

        plan = self._build_query_plan(df_cfg)
        if plan.empty:
            print("No valid (stg_view, stg_tbl) groups could be built from the DRAFT rows.")
            return pd.DataFrame()

        existing_tables = self._existing_tables()
        plan = plan.copy()
        plan["write_mode"] = plan["stg_tbl"].apply(
            lambda t: "insert" if t in existing_tables else "create"
        )

        if not confirm_apply:
            print(f"[DRY RUN] update(): {len(plan)} stg_tbl group(s) would be processed. "
                  f"Set confirm_apply=True to execute.")
            return plan[["stg_view", "stg_tbl", "write_mode", "afs_uid_list"]]

        run_id = self._new_run_id()
        results: List[Dict[str, Any]] = []
        for row in plan.itertuples(index=False):
            print(f"[{run_id}] UPDATE {row.stg_view} -> {row.stg_tbl} ({row.write_mode})...")
            res = self._execute_group(row.stg_tbl, row.indicator_src_query, row.write_mode)
            res["stg_view"] = row.stg_view
            res["stg_tbl"] = row.stg_tbl
            res["afs_uid_list"] = row.afs_uid_list
            results.append(res)
            status = res["job_status"]
            print(f"  -> {status}" + (f": {res['error_message']}" if status == "FAILED" else ""))

        self._write_log(run_id, "UPDATE", results)

        if confirm_status_update:
            successful_uids = [
                uid
                for r in results if r["job_status"] == "SUCCESS"
                for uid in r["afs_uid_list"]
            ]
            n_updated = self._mark_reviewed(successful_uids)
            print(f"[{run_id}] Marked {n_updated} row(s) DRAFT -> REVIEW.")
        else:
            print(f"[{run_id}] confirm_status_update=False: status left as DRAFT for all rows, "
                  f"regardless of job outcome.")

        return pd.DataFrame(results)

    # ------------------------------------------------------------------
    # Public entry point — RECONSTRUCT
    # ------------------------------------------------------------------

    def reconstruct(
        self,
        stg_view: Optional[str] = None,
        confirm_apply: bool = False,
    ) -> pd.DataFrame:
        """ACTIVE / REVIEW / INACTIVE rows only. Always CREATE OR REPLACE
        — the full stg_tbl is rebuilt from current source data, no
        appending. Never touches status.

        stg_view=None rebuilds every (stg_view, stg_tbl) group found;
        pass a specific stg_view to rebuild only the stg_tbl it feeds,
        without affecting any other stg_tbl.

        Nothing is executed until confirm_apply=True; calling this with
        the default returns a preview of the groups that would run.
        """
        self.validate()

        df_cfg = self._load_cfg(statuses=["ACTIVE", "REVIEW", "INACTIVE"], stg_view=stg_view)
        if df_cfg.empty:
            scope = f"stg_view='{stg_view}'" if stg_view else "any status in (ACTIVE, REVIEW, INACTIVE)"
            print(f"No cfg rows found for {scope}. Nothing to reconstruct.")
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
            return preview[["stg_view", "stg_tbl", "write_mode", "afs_uid_list"]]

        run_id = self._new_run_id()
        results: List[Dict[str, Any]] = []
        for row in plan.itertuples(index=False):
            print(f"[{run_id}] RECONSTRUCT {row.stg_view} -> {row.stg_tbl} (create_or_replace)...")
            res = self._execute_group(row.stg_tbl, row.indicator_src_query, "create_or_replace")
            res["stg_view"] = row.stg_view
            res["stg_tbl"] = row.stg_tbl
            res["afs_uid_list"] = row.afs_uid_list
            results.append(res)
            status = res["job_status"]
            print(f"  -> {status}" + (f": {res['error_message']}" if status == "FAILED" else ""))

        self._write_log(run_id, "RECONSTRUCT", results)
        return pd.DataFrame(results)

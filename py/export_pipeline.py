# -*- coding: utf-8 -*-
"""export_pipeline.py

ExportPipeline — the BigQuery-only workflow that aggregates the per-source
`stg_*_vw` views (raw, "unprocessed") and the `stg_*_tbl` tables
(indicator-filtered, "processed") into two independent output tables and
exports each of them to GCS.

This runs strictly *after* `StagePipeline` has already built/refreshed
the `stg_*_tbl` tables (and after the `stg_*_vw` views that feed them
already exist) — this class never writes to a `stg_*` table or view,
it only reads from them. It shares `cfg_stage_tbl` with `StagePipeline`
(same table, read-only here too) but does not touch its `status`
column and does not create or alter it.

The pipeline is split into two independent workflows, each with its own
`validate_*()` and `run_*()` entry point and its own step methods. Always
a full rebuild (no incremental mode); each `run_*()` runs its steps in
order and stops at the first failure.

SRC workflow — `run_src()`
  1. `build_src_view()`       CREATE OR REPLACE VIEW `exp_src_all_vw`: UNION
                              ALL of every `stg_{code}_vw` (the raw view
                              `stg_{code}_tbl` reads from, before any
                              indicator-level `filter_string`) whose
                              `afs_source_code` appears (non-null, any
                              status) in `cfg_stage_tbl`.
  2. `build_src_all_table()`  CREATE OR REPLACE TABLE `exp_src_all_tbl` as a
                              straight `SELECT *` of the view — no
                              scaffold, no gap-filling, and no ORDER BY /
                              CLUSTER BY / PARTITION BY (performance
                              tuning comes later).
  3. `export_src_to_gcs()`    Extract job: `exp_src_all_tbl` -> ONE Avro
                              file, `{gcs_base_uri}/exp_src_all_tbl.avro`,
                              overwritten on every run. Compression is
                              BigQuery's default (none) unless
                              `src_export_compression` is set.

STG workflow — `run_stg()`  (not reworked yet; still exports Parquet)
  1. `build_long_view()`       VIEW `exp_stg_all_long_vw`: UNION ALL of every
                               `stg_{code}_tbl`.
  2. `build_scaffold_table()`  TABLE `exp_ref_scaffold_cyi_tbl`: every
                               m49_code x year x ACTIVE/REVIEW indicator.
  3. `build_stg_all_table()`   TABLE `exp_stg_all_long_tbl`: scaffold FULL
                               JOIN long view (gap-filled; orphaned
                               long-view rows are kept, join keys are
                               COALESCEd).
  4. `export_stg_to_gcs()`     Extract job: `exp_stg_all_long_tbl` -> ONE
                               Parquet file,
                               `{gcs_base_uri}/exp_stg_all_long_tbl.parquet`.

Both exports use a BigQuery extract job (`extract_table`), not the
EXPORT DATA statement, because EXPORT DATA only accepts a wildcard URI
(sharded output). An extract job can write one fixed file, limited to
1 GB of table data per file — if a table outgrows that, the job fails and
the export has to move to a sharded (wildcard) form.

Validation only checks table names: `cfg_stage_tbl` (and, for STG, the
scope table) must exist; the expected `stg_*_vw` / `stg_*_tbl` objects are
looked up (missing ones are skipped with a warning, none found raises);
the log table is created if it doesn't exist. There is no schema check:
UNION ALL is positional, so every unioned view/table must have identical
columns in identical order.

Nothing is executed until `confirm_apply=True` is passed explicitly, on
the step methods or `run_src()` / `run_stg()`; without it they return a
preview of the SQL/plan for review.
"""

from __future__ import annotations

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
STEP_EXPORT_STG_GCS = "EXPORT_STG_GCS"
STEP_EXPORT_SRC_GCS = "EXPORT_SRC_GCS"


class ExportPipeline:
    """The BigQuery-only pipeline that builds two independent output
    tables — src (union of stg_*_vw) and stg (union of stg_*_tbl,
    gap-filled) — and exports each to GCS. Each has its own
    validate_*() / run_*() entry point (see the module docstring).

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
        src_export_compression: Optional[str] = None,
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
        # Avro compression for the src export. None = BigQuery's default
        # (NONE, i.e. uncompressed). Avro supports "SNAPPY" or "DEFLATE".
        self.src_export_compression = src_export_compression

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
        return f"{self.gcs_base_uri.rstrip('/')}/{self.src_table_name}.avro"

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
    # Log table
    # ------------------------------------------------------------------

    @staticmethod
    def _log_table_schema() -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("step", "STRING"),           # BUILD_LONG_VIEW / BUILD_SRC_VIEW / BUILD_SCAFFOLD_TABLE / BUILD_STG_ALL_TABLE / BUILD_SRC_TABLE / EXPORT_STG_GCS / EXPORT_SRC_GCS
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

    # ------------------------------------------------------------------
    # Validation helpers (table names only) + discovery
    # ------------------------------------------------------------------

    def _check_base_tables(self, extra_required: Dict[str, str]) -> None:
        """Table-name checks only: cfg_stage_tbl (plus any extra tables
        the calling workflow needs) must exist — this class never
        creates them. The log table is created if it doesn't exist."""
        existing = self._existing_tables()
        required = {"Config table": self.cfg_stage_table_name, **extra_required}
        for label, name in required.items():
            if name not in existing:
                raise RuntimeError(
                    f"{label} '{name}' not found in "
                    f"{self.project_id}.{self.dataset_id}. ExportPipeline does not create it."
                )
            print(f"{label} '{name}' found.")

        if self.log_table_name not in existing:
            print(f"Log table '{self.log_table_name}' not found — creating it.")
            self._create_log_table()
        else:
            print(f"Log table '{self.log_table_name}' found.")

    def _discover_existing(self, name_fn, label: str) -> List[str]:
        """All non-null, distinct afs_source_code in cfg_stage_tbl,
        turned into their expected object name via `name_fn` (same
        naming rule StagePipeline uses), keeping only the ones that
        physically exist. A source configured but not yet built is
        skipped with a warning rather than failing the whole run."""
        codes = self._distinct_source_codes()
        expected = {name_fn(code) for code in codes}
        existing = self._existing_tables()

        found = sorted(expected & existing)
        missing = sorted(expected - existing)
        if missing:
            print(f"WARNING: {len(missing)} expected {label} not found and will be skipped: {missing}")
        return found

    def _discover_stg_tables(self) -> List[str]:
        """Existing stg_{code}_tbl tables (indicator-filtered)."""
        return self._discover_existing(self._stg_tbl_name, "stg_tbl")

    def _discover_stg_views(self) -> List[str]:
        """Existing stg_{code}_vw views (raw, before any indicator-level
        filter_string is applied)."""
        return self._discover_existing(self._stg_view_name, "stg_vw")

    # ------------------------------------------------------------------
    # Shared job runner
    # ------------------------------------------------------------------

    def _run_job(self, sql: str, count_table: Optional[str] = None) -> Dict[str, Any]:
        """Runs one BQ job. Never raises — failures are captured and
        returned so the caller (_run_sequence) can decide whether to stop.
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

    def _run_extract_job(
        self,
        table_name: str,
        uri: str,
        destination_format: str,
        compression: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Runs one BigQuery extract job (table -> ONE file at `uri`).
        Same never-raises contract as _run_job(): failures are captured
        in the returned dict. An extract job overwrites an existing
        file at the same URI. `compression=None` leaves the option
        unset, i.e. BigQuery's default (NONE). rows_affected is the
        table's num_rows, since the extract can't change the row count.
        """
        result: Dict[str, Any] = {
            "job_status": "FAILED",
            "error_message": None,
            "bq_job_id": None,
            "rows_affected": None,
        }
        job = None
        try:
            table = self.bq_client.get_table(self._table_fqn(table_name))
            job_config = bigquery.ExtractJobConfig(destination_format=destination_format)
            if compression:
                job_config.compression = compression
            job = self.bq_client.extract_table(
                table, uri, job_config=job_config, location=table.location
            )
            job.result()  # blocks until done; raises on job error
            result["bq_job_id"] = job.job_id
            result["job_status"] = "SUCCESS"
            result["rows_affected"] = table.num_rows
        except (GoogleAPIError, Exception) as exc:  # noqa: BLE001 - broad on purpose, see _run_job
            if job is not None:
                result["bq_job_id"] = job.job_id
            result["error_message"] = self._format_error(exc)
        return result

    def _export_one_table(
        self,
        method_name: str,
        step: str,
        table_name: str,
        uri: str,
        destination_format: str,
        compression: Optional[str],
        confirm_apply: bool,
    ) -> Dict[str, Any]:
        """Shared body of export_src_to_gcs() / export_stg_to_gcs()."""
        if not confirm_apply:
            print(
                f"[DRY RUN] {method_name}(): would extract `{table_name}` to '{uri}' "
                f"(single {destination_format} file, compression="
                f"{compression or 'BigQuery default (NONE)'}, overwritten if it exists). "
                f"Set confirm_apply=True to execute."
            )
            return {
                "table_name": table_name,
                "gcs_uri": uri,
                "format": destination_format,
                "compression": compression,
            }

        result = self._run_extract_job(table_name, uri, destination_format, compression)
        result["step"] = step
        result["object_name"] = uri
        return result

    def _run_sequence(self, steps) -> pd.DataFrame:
        """Runs (name, method) steps in order with confirm_apply=True,
        stops at the first failure, writes one log row per step run."""
        run_id = self._new_run_id()
        results: List[Dict[str, Any]] = []
        total = len(steps)

        for n, (step_name, step_fn) in enumerate(steps, start=1):
            print(f"[{run_id}] STEP {n}/{total}: {step_name}...")
            r = step_fn(confirm_apply=True)
            results.append(r)
            print(f"  -> {r['job_status']}" + (f": {r['error_message']}" if r["job_status"] == "FAILED" else ""))
            if r["job_status"] != "SUCCESS":
                print(f"[{run_id}] Stopped after STEP {n}/{total} ({step_name}) failure.")
                break

        self._write_log(run_id, results)
        return pd.DataFrame(results)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def _write_log(self, run_id: str, results: List[Dict[str, Any]]) -> None:
        """One log row per step, success or failure — a full execution
        history, not just an error log. Granularity is per-step here
        (not per-source-group like StagePipeline's log) since these
        steps are sequential, not independent units of work."""
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

    # ==================================================================
    # SRC WORKFLOW — exp_src_all_vw -> exp_src_all_tbl -> Avro on GCS
    # ==================================================================

    def validate_src(self) -> None:
        """Table-name checks only: cfg_stage_tbl exists, the log table
        exists (created if not), and at least one expected stg_*_vw
        exists (missing ones are warned about and skipped)."""
        self._check_base_tables({})
        views = self._discover_stg_views()
        if not views:
            raise RuntimeError("No stg_*_vw found for the source codes in cfg_stage_tbl.")
        print(f"{len(views)} stg_vw found.")

    # ------------------------------------------------------------------
    # SRC step 1 — src view (union of stg_*_vw, unprocessed)
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
        otherwise: always a full replace, no rows_affected
        (view holds no data of its own). No schema check: UNION ALL is
        positional, so every stg_vw must have identical columns in
        identical order.
        """
        stg_views = self._discover_stg_views()
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
    # SRC step 2 — src_all table (straight materialization, no scaffold)
    # ------------------------------------------------------------------

    def _build_src_all_table_query(self) -> str:
        return f"SELECT * FROM `{self._src_view_fqn}`"

    def build_src_all_table(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """CREATE OR REPLACE TABLE exp_src_all_tbl (no CLUSTER BY /
        PARTITION BY for now). Unlike build_stg_all_table(), this is a straight materialization
        of exp_src_all_vw — no scaffold, no FULL JOIN, no gap-filling —
        so it holds what's physically present across every stg_vw
        (from start_year onward, the lower bound exp_src_all_vw applies). Reads
        exp_src_all_vw, so build_src_view() must already have been run
        successfully. Always a full rebuild — no incremental mode.
        """
        final_query = self._build_src_all_table_query()

        if not confirm_apply:
            print(
                f"[DRY RUN] build_src_all_table(): would (re)build `{self.src_table_name}`. "
                f"Set confirm_apply=True to execute."
            )
            return {"sql": final_query}

        sql = f"CREATE OR REPLACE TABLE `{self._src_table_fqn}`\nAS\n{final_query}"
        result = self._run_job(sql, count_table=self.src_table_name)
        result["step"] = STEP_BUILD_SRC_TABLE
        result["object_name"] = self.src_table_name
        return result

    # ------------------------------------------------------------------
    # SRC step 3 — export exp_src_all_tbl to GCS (single Avro file)
    # ------------------------------------------------------------------

    def export_src_to_gcs(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """Exports exp_src_all_tbl as ONE Avro file at
        `{gcs_base_uri}/{src_table_name}.avro` (overwritten on every
        run). Requires build_src_all_table() to have been run.

        Uses an extract job (not EXPORT DATA, which requires a wildcard
        URI); single-file extracts are capped at 1 GB of table data —
        the job fails beyond that. Compression is BigQuery's default
        (NONE) unless src_export_compression is set to "SNAPPY" or
        "DEFLATE". Avro logical types are left at BigQuery's default
        (off), so e.g. TIMESTAMP columns are written as raw long values.
        """
        return self._export_one_table(
            "export_src_to_gcs", STEP_EXPORT_SRC_GCS,
            self.src_table_name, self._src_all_export_uri,
            bigquery.DestinationFormat.AVRO, self.src_export_compression,
            confirm_apply,
        )

    def run_src(self, confirm_apply: bool = False) -> pd.DataFrame:
        """build_src_view -> build_src_all_table -> export_src_to_gcs,
        stopping at the first failure. Calls validate_src() first.
        Nothing is executed until confirm_apply=True; by default this
        only validates and prints the plan (use each step's own dry run
        for SQL detail)."""
        self.validate_src()

        if not confirm_apply:
            print(
                "[DRY RUN] run_src(): would run build_src_view -> build_src_all_table -> "
                f"export_src_to_gcs() -> '{self._src_all_export_uri}'. Set confirm_apply=True to execute."
            )
            return pd.DataFrame()

        return self._run_sequence([
            ("build_src_view", self.build_src_view),
            ("build_src_all_table", self.build_src_all_table),
            ("export_src_to_gcs", self.export_src_to_gcs),
        ])

    # ==================================================================
    # STG WORKFLOW — long view + scaffold -> exp_stg_all_long_tbl -> Parquet
    # (logic unchanged from before, apart from dropping the schema check;
    #  to be reworked after the SRC workflow)
    # ==================================================================

    def validate_stg(self) -> None:
        """Table-name checks only: cfg_stage_tbl and the scope table
        exist, the log table exists (created if not), and at least one
        expected stg_*_tbl exists (missing ones are warned about and
        skipped)."""
        self._check_base_tables({"Scope table": self.scope_table_name})
        tables = self._discover_stg_tables()
        if not tables:
            raise RuntimeError("No stg_*_tbl found for the source codes in cfg_stage_tbl.")
        print(f"{len(tables)} stg_tbl found.")

    # ------------------------------------------------------------------
    # STG step 1 — long view (union of stg_*_tbl, processed)
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

        No schema check: UNION ALL is positional, so every stg_tbl must
        have identical columns in identical order.
        """
        stg_tables = self._discover_stg_tables()
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
    # STG step 2 — scaffold table
    # ------------------------------------------------------------------

    def _build_scaffold_query(self) -> str:
        """m49_code x year(start_year..current_year) x ACTIVE/REVIEW
        indicator. No ORDER BY (performance tuning comes later). Year
        upper bound is computed fresh at query time
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
                f"[DRY RUN] build_scaffold_table(): would (re)build `{self.scaffold_table_name}`. "
                f"Set confirm_apply=True to execute."
            )
            return {"sql": scaffold_query}

        sql = f"CREATE OR REPLACE TABLE `{self._scaffold_table_fqn}`\nAS\n{scaffold_query}"
        result = self._run_job(sql, count_table=self.scaffold_table_name)
        result["step"] = STEP_BUILD_SCAFFOLD_TABLE
        result["object_name"] = self.scaffold_table_name
        return result

    # ------------------------------------------------------------------
    # STG step 3 — stg_all table (scaffold FULL JOIN long view, gap-filled)
    # ------------------------------------------------------------------

    def _build_stg_all_table_query(self) -> str:
        """exp_ref_scaffold_cyi_tbl FULL JOIN exp_stg_all_long_vw. FULL
        (not LEFT) so a long-view row with no matching scaffold
        combination — wrong m49_code, a year after
        current_year (years before start_year are already dropped by the union view), or an indicator no longer ACTIVE/REVIEW — still
        surfaces instead of being dropped. The three join keys are
        COALESCEd from both sides so a long-view-only row still carries
        non-NULL keys (without this, those orphaned rows would come
        through with NULL keys, since the scaffold side is all NULL).
        """
        return f"""
        SELECT
          COALESCE(scaffold.afs_m49_code, long_vw.afs_m49_code) AS afs_m49_code,
          COALESCE(scaffold.afs_indicator_uid, long_vw.afs_indicator_uid) AS afs_indicator_uid,
          COALESCE(scaffold.afs_year, long_vw.afs_year) AS afs_year,
          scaffold.* EXCEPT (afs_m49_code, afs_indicator_uid, afs_year),
          long_vw.* EXCEPT (afs_m49_code, afs_indicator_uid, afs_year)
        FROM `{self._scaffold_table_fqn}` AS scaffold
        FULL JOIN `{self._long_view_fqn}` AS long_vw
          ON scaffold.afs_m49_code = long_vw.afs_m49_code
          AND scaffold.afs_indicator_uid = long_vw.afs_indicator_uid
          AND scaffold.afs_year = long_vw.afs_year
        """

    def build_stg_all_table(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """CREATE OR REPLACE TABLE exp_stg_all_long_tbl (no CLUSTER BY /
        PARTITION BY for now). Reads exp_ref_scaffold_cyi_tbl
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
                f"[DRY RUN] build_stg_all_table(): would (re)build `{self.stg_all_table_name}`. "
                f"Set confirm_apply=True to execute."
            )
            return {"sql": final_query}

        sql = f"CREATE OR REPLACE TABLE `{self._stg_all_table_fqn}`\nAS\n{final_query}"
        result = self._run_job(sql, count_table=self.stg_all_table_name)
        result["step"] = STEP_BUILD_STG_ALL_TABLE
        result["object_name"] = self.stg_all_table_name
        return result

    # ------------------------------------------------------------------
    # STG step 4 — export exp_stg_all_long_tbl to GCS (single Parquet file)
    # ------------------------------------------------------------------

    def export_stg_to_gcs(self, confirm_apply: bool = False) -> Dict[str, Any]:
        """Exports exp_stg_all_long_tbl as ONE Parquet file at
        `{gcs_base_uri}/{stg_all_table_name}.parquet` (overwritten on
        every run). Requires build_stg_all_table() to have been run.

        Uses an extract job (not EXPORT DATA, which requires a wildcard
        URI); single-file extracts are capped at 1 GB of table data —
        the job fails beyond that.
        """
        return self._export_one_table(
            "export_stg_to_gcs", STEP_EXPORT_STG_GCS,
            self.stg_all_table_name, self._stg_all_export_uri,
            bigquery.DestinationFormat.PARQUET, None,
            confirm_apply,
        )

    def run_stg(self, confirm_apply: bool = False) -> pd.DataFrame:
        """build_long_view -> build_scaffold_table -> build_stg_all_table
        -> export_stg_to_gcs, stopping at the first failure. Calls
        validate_stg() first. Nothing is executed until
        confirm_apply=True; by default this only validates and prints
        the plan."""
        self.validate_stg()

        if not confirm_apply:
            print(
                "[DRY RUN] run_stg(): would run build_long_view -> build_scaffold_table -> "
                f"build_stg_all_table -> export_stg_to_gcs() -> '{self._stg_all_export_uri}'. "
                "Set confirm_apply=True to execute."
            )
            return pd.DataFrame()

        return self._run_sequence([
            ("build_long_view", self.build_long_view),
            ("build_scaffold_table", self.build_scaffold_table),
            ("build_stg_all_table", self.build_stg_all_table),
            ("export_stg_to_gcs", self.export_stg_to_gcs),
        ])

"""config_sheet_syncer

Shared class for syncing a Google Sheet <-> BigQuery config table pair.

Used by multiple Colab notebooks (e.g. the source-config sync notebook
and the stage-config notebook) — each notebook does its own auth/client
bootstrapping and env config, then instantiates `ConfigSheetSyncer` with
the constructor arguments that fit its use case. Both follow the same
"edit main directly" workflow:

    pull (cfg_table -> Sheet) -> manual edit in Sheet -> push (Sheet -> cfg_table)

    - row_key_column: which column uniquely identifies a row, used when
      comparing Sheet vs. cfg_table rows in validate(). Defaults to
      "code"; pass a different column (e.g. "afs_source_code",
      "afs_uid") if that's not the right key for a given sheet.

See the class and method docstrings below for full details.
"""

from datetime import datetime
from typing import Dict, Optional

import gspread
import pandas as pd
from google.cloud import bigquery


class ConfigSheetSyncer:
    """Manages the data flow between a Google Sheet (human-edited config)
    and a BigQuery config table, including pre-sync validation and a
    sync log.

    This is the SAME class used for both the "source" config sync and the
    "stage" config sync — the two use cases differ only in the
    constructor arguments (sheet/table names, and row_key_column, the
    column that uniquely identifies a row when comparing Sheet vs.
    cfg_table row-by-row).

    Each instance is bound to:
        - one worksheet (where a human edits config)
        - one log worksheet (records this instance's pull/push history)
        - one BigQuery native table (the landed config table)

    Create multiple instances to manage different configs (source cfg,
    stage cfg, ...) independently.
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    def __init__(
        self,
        main_sheet_name: str,
        log_sheet_name: str,
        spreadsheet: gspread.Spreadsheet,
        bq_client: bigquery.Client,
        dataset_id: str,
        external_cfg_tbl_name: str,
        cfg_table_name: str,
        project_id: Optional[str] = None,
        row_key_column: str = "code",
    ):
        """
        Args:
            spreadsheet: a gspread Spreadsheet object (multiple instances
                can share the same Spreadsheet and just point at different
                worksheets).
            bq_client: BigQuery Client.
            dataset_id: the BQ dataset this config table lives in.
            external_cfg_tbl_name: external table name, e.g. "cfg_source_gstbl".
            cfg_table_name: internal cfg table name, e.g. "cfg_source_tbl".
            main_sheet_name: worksheet name this instance edits, e.g. "main".
            log_sheet_name: this instance's dedicated log worksheet name.
            project_id: BQ project ID. Defaults to bq_client.project.
            row_key_column: the column used as a unique row key when
                comparing Sheet vs. cfg_table rows in validate(). Defaults
                to "code"; pass something like "afs_source_code" or
                "afs_uid" for configs that use a different key.
        """
        self.sh = spreadsheet
        self.bq_client = bq_client
        self.project_id = project_id or bq_client.project
        self.dataset_id = dataset_id
        self.cfg_table_name = cfg_table_name
        self.ext_table_name = external_cfg_tbl_name
        self.row_key_column = row_key_column

        self.main_sheet_name = main_sheet_name
        self.log_sheet_name = log_sheet_name

        # 1. Main worksheet handle. Intentionally NOT auto-created: if the
        #    name is wrong or the sheet doesn't exist, we want a loud error
        #    instead of silently creating an empty sheet that could be
        #    mistaken for real config data.
        self.main_ws = self.sh.worksheet(self.main_sheet_name)

        # 2. Log worksheet handle.
        self.log_ws = self._init_or_get_log_worksheet()

    # ------------------------------------------------------------------
    # Small internal helpers
    # ------------------------------------------------------------------
    @property
    def cfg_table_ref(self) -> str:
        """Fully-qualified cfg_table reference, e.g. project.dataset.table"""
        return f"{self.project_id}.{self.dataset_id}.{self.cfg_table_name}"

    @property
    def ext_table_ref(self) -> str:
        """Fully-qualified external table reference, e.g. project.dataset.table_ext"""
        return f"{self.project_id}.{self.dataset_id}.{self.ext_table_name}"

    def _init_or_get_log_worksheet(self) -> gspread.Worksheet:
        """[Internal] Get the log worksheet, creating it with headers if missing.

        Log schema (only records the sync action itself, no business data):
            execution_time | direction | sheet_name | cfg_table | status | detail
        """
        try:
            return self.sh.worksheet(self.log_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Creating new log worksheet: '{self.log_sheet_name}'...")
            log_ws = self.sh.add_worksheet(
                title=self.log_sheet_name, rows=1000, cols=10
            )
            headers = [
                "execution_time",
                "direction",     # "pull" (tbl->Sheet) or "push" (Sheet->tbl)
                "sheet_name",
                "cfg_table",
                "status",        # SUCCESS / FAILED / SKIPPED
                "detail",        # row count, error message, etc.
            ]
            log_ws.append_row(headers)
            return log_ws

    def _log_sync(self, direction: str, status: str, detail: str = ""):
        """Record one pull / push log entry."""
        execution_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_row = [
            execution_time,
            direction,
            self.main_sheet_name,
            self.cfg_table_name,
            status,
            detail,
        ]
        self.log_ws.append_row(log_row)
        print(f"[Log] Recorded a '{direction}' entry in '{self.log_sheet_name}'")

    # ------------------------------------------------------------------
    # Raw reads (no cleaning / indexing / filtering)
    # ------------------------------------------------------------------
    def get_cfg_dataframe(self, raw_strings: bool = True) -> pd.DataFrame:
        """Read the main worksheet into a DataFrame, with no processing.

        Args:
            raw_strings: if True (default), read every cell as a plain
                string via get_all_values() (header = first row). This
                avoids gspread's get_all_records(), which auto-casts
                numeric-looking cells to int/float and would silently turn
                a value like "007" into the integer 7, dropping the
                leading zeros. Set raw_strings=False to use
                get_all_records() instead and let gspread infer types.

        Returns:
            The DataFrame as read from the Sheet.
        """
        if raw_strings:
            values = self.main_ws.get_all_values()
            df = pd.DataFrame(values[1:], columns=values[0]) if values else pd.DataFrame()
        else:
            records = self.main_ws.get_all_records()
            df = pd.DataFrame(records)
        return df

    def get_cfg_dataframe_from_bq(self) -> pd.DataFrame:
        """Query the full cfg_table (BQ) into a DataFrame, with no processing."""
        return self.bq_client.query(f"SELECT * FROM `{self.cfg_table_ref}`").result().to_dataframe()

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def _validate_dataframe(self, df: pd.DataFrame, source_name: str) -> list:
        """Basic checks: not empty, no blank column names, no duplicate column names.

        Returns:
            A list of issue strings; an empty list means the check passed.
        """
        issues = []

        if df.empty:
            issues.append(f"{source_name} is empty (no data rows)")
            return issues

        cols = list(df.columns)

        empty_cols = [i for i, c in enumerate(cols) if str(c).strip() == ""]
        if empty_cols:
            issues.append(f"{source_name} has blank column name(s) at position(s) (0-based): {empty_cols}")

        seen: Dict[str, int] = {}
        dup_cols = set()
        for c in cols:
            seen[c] = seen.get(c, 0) + 1
            if seen[c] > 1:
                dup_cols.add(c)
        if dup_cols:
            issues.append(f"{source_name} has duplicate column name(s): {sorted(dup_cols)}")

        return issues

    def _compare_schema_and_rows(
        self, sheet_df: pd.DataFrame, bq_df: pd.DataFrame
    ) -> list:
        """Compare Sheet vs. cfg_table column structure, and row-level
        differences keyed on self.row_key_column.

        Returns:
            A list of informational notes. These differences aren't
            necessarily "errors", but are worth surfacing before a sync.
        """
        notes = []

        # --- Column structure ---
        sheet_cols = set(sheet_df.columns)
        bq_cols = set(bq_df.columns)

        only_in_sheet_cols = sheet_cols - bq_cols
        only_in_bq_cols = bq_cols - sheet_cols

        if only_in_sheet_cols or only_in_bq_cols:
            notes.append("Column structure differs:")
            if only_in_sheet_cols:
                notes.append(f"    Columns only in Sheet: {sorted(only_in_sheet_cols)}")
            if only_in_bq_cols:
                notes.append(f"    Columns only in cfg_table: {sorted(only_in_bq_cols)}")
        else:
            notes.append("Column structure matches")

        # --- Row differences (keyed on row_key_column) ---
        key_col = self.row_key_column
        if key_col not in sheet_df.columns or key_col not in bq_df.columns:
            notes.append(f"Cannot compare rows: '{key_col}' column is missing from Sheet or cfg_table")
            return notes

        sheet_keys = set(sheet_df[key_col].astype(str))
        bq_keys = set(bq_df[key_col].astype(str))

        only_in_sheet_rows = sheet_keys - bq_keys
        only_in_bq_rows = bq_keys - sheet_keys

        if only_in_sheet_rows or only_in_bq_rows:
            notes.append(f"Row content differs (compared on '{key_col}'):")
            if only_in_sheet_rows:
                notes.append(f"    '{key_col}' values only in Sheet: {sorted(only_in_sheet_rows)}")
            if only_in_bq_rows:
                notes.append(f"    '{key_col}' values only in cfg_table: {sorted(only_in_bq_rows)}")
        else:
            notes.append(f"Row content matches (same set of '{key_col}' values on both sides)")

        return notes

    def validate(self) -> dict:
        """Standalone pre-sync validation — can be called any time on its
        own. Performs no sync action.

        Checks:
            - hard_issues: is Sheet / cfg_table empty, blank/duplicate column names
            - notes: Sheet vs. cfg_table column structure and row-level differences

        Returns:
            {
                "sheet_df": DataFrame,
                "bq_df": DataFrame,
                "hard_issues": list[str],   # non-empty means there's a hard problem
                "notes": list[str],         # informational only, doesn't block syncing
            }
        """
        print("[Validate] Starting validation ...")

        sheet_df = self.get_cfg_dataframe()
        bq_df = self.get_cfg_dataframe_from_bq()

        hard_issues = []
        hard_issues += self._validate_dataframe(sheet_df, "Sheet")
        hard_issues += self._validate_dataframe(bq_df, "cfg_table")

        if hard_issues:
            print("[Validate] Found the following problem(s), please fix first:")
            for issue in hard_issues:
                print(f"    - {issue}")
            return {
                "sheet_df": sheet_df,
                "bq_df": bq_df,
                "hard_issues": hard_issues,
                "notes": [],
            }

        notes = self._compare_schema_and_rows(sheet_df, bq_df)
        print("[Validate] Sheet vs. cfg_table diff report:")
        for note in notes:
            print(f"    {note}")

        print("[Validate] Basic validation passed (not empty, valid column names)")
        return {
            "sheet_df": sheet_df,
            "bq_df": bq_df,
            "hard_issues": hard_issues,
            "notes": notes,
        }

    # ------------------------------------------------------------------
    # pull: cfg_table (BQ) -> Sheet
    # ------------------------------------------------------------------
    def pull(self, force: bool = False):
        """cfg_table -> Sheet (read everything, overwrite the worksheet).

        Workflow stage: before a human starts editing the Sheet, pull down
        the latest state from cfg_table (e.g. after a pipeline run updated
        it), so the Sheet reflects the current state.

        Args:
            force: True skips validation entirely and syncs directly.
                False (default) calls validate() first, and aborts if
                hard_issues are found.
        """
        if force:
            bq_df = self.get_cfg_dataframe_from_bq()
        else:
            result = self.validate()
            if result["hard_issues"]:
                self._log_sync("pull", "FAILED", detail="; ".join(result["hard_issues"]))
                raise ValueError(f"pull() validation failed: {result['hard_issues']}")
            bq_df = result["bq_df"]

        print(
            f"[Pull] Syncing {self.cfg_table_name} -> '{self.main_sheet_name}' ..."
            + (" (force=True, validation skipped)" if force else "")
        )
        try:
            df = bq_df.astype(object).where(pd.notnull(bq_df), "")
            # DataFrame.map (pandas >= 2.1) replaces the deprecated applymap.
            df = df.map(lambda v: str(v) if not isinstance(v, (str, int, float, bool)) else v)

            values = [df.columns.tolist()] + df.values.tolist()
            self.main_ws.clear()
            self.main_ws.update(values, value_input_option="USER_ENTERED")

            print(f"[Pull] Done. Wrote {len(df)} rows back to '{self.main_sheet_name}'")
            self._log_sync("pull", "SUCCESS", detail=f"{len(df)} rows" + (" (forced)" if force else ""))

        except Exception as e:
            print(f"[Pull] Failed: {e}")
            self._log_sync("pull", "FAILED", detail=str(e))
            raise

    # ------------------------------------------------------------------
    # External table: Sheet -> BQ external table (schema pointer only)
    # ------------------------------------------------------------------
    def create_external_table(self, ext_table_id: Optional[str] = None):
        """Create/replace the BQ external table (GOOGLE_SHEETS source)
        pointing at this worksheet.

        This is a standalone, public method — push() does NOT call it
        automatically. Call it by hand the first time you set things up,
        or whenever the Sheet's URL/worksheet/column structure changes.

        No autodetect: autodetect would drop any column that's entirely
        empty from the inferred schema, silently making that column
        "disappear" from BQ. Instead, this reads the Sheet's header row
        explicitly and builds the schema column by column, all as STRING,
        so every column (even an entirely empty one) is preserved.

        Args:
            ext_table_id: temporarily overrides the external table name
                (defaults to external_cfg_tbl_name / self.ext_table_name
                from the constructor). If passed, also updates
                self.ext_table_name, so subsequent push() calls use it too.
        """
        if ext_table_id:
            self.ext_table_name = ext_table_id

        sheet_url = self.sh.url

        # Read the Sheet's header row (not autodetect) and build a STRING
        # schema column by column.
        header_row = self.main_ws.row_values(1)
        schema = []
        name_counts: Dict[str, int] = {}
        for i, raw_name in enumerate(header_row):
            name = str(raw_name).strip()
            if not name:
                # Blank header — give it a placeholder name so this
                # column's position isn't lost.
                name = f"col_{i + 1}"
            if name in name_counts:
                name_counts[name] += 1
                name = f"{name}_{name_counts[name]}"
            else:
                name_counts[name] = 0
            schema.append(bigquery.SchemaField(name, "STRING"))

        external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")
        external_config.source_uris = [sheet_url]
        external_config.options.skip_leading_rows = 1
        # range selects which worksheet (tab) to read
        external_config.options.range = self.main_sheet_name
        external_config.autodetect = False

        table = bigquery.Table(self.ext_table_ref, schema=schema)
        table.external_data_configuration = external_config

        self.bq_client.delete_table(self.ext_table_ref, not_found_ok=True)
        created = self.bq_client.create_table(table)
        print(f"External table ready: {created.full_table_id} -> {sheet_url} ({self.main_sheet_name})")
        print(f"   Schema ({len(schema)} columns, all STRING): {[f.name for f in schema]}")
        return created

    # ------------------------------------------------------------------
    # push: Sheet -> external table -> cfg_table (BQ)
    # ------------------------------------------------------------------
    def push(self, force: bool = False):
        """Sheet -> external table -> cfg_table (CREATE OR REPLACE; schema
        fully follows the Sheet).

        Requires the external table to already exist (set up ahead of
        time via create_external_table()). push() itself does NOT
        create/refresh the external table — it only runs:
            CREATE OR REPLACE TABLE cfg_table AS SELECT * FROM ext_table

        This means cfg_table's column structure is fully aligned with the
        external table (i.e. whatever it looked like the last time
        create_external_table() ran). If you've added/removed Sheet
        columns, call create_external_table() again first to refresh the
        external table, then push() — otherwise the structural change
        won't take effect. Editing cell values only (no structural
        change) is unaffected, since the external table reads the Sheet
        live.

        Workflow stage: after a human finishes editing the Sheet, push
        it to cfg_table.

        Args:
            force: True skips validation entirely and syncs directly.
                False (default) calls validate() first, and aborts if
                hard_issues are found.
        """
        if not force:
            result = self.validate()
            if result["hard_issues"]:
                self._log_sync("push", "FAILED", detail="; ".join(result["hard_issues"]))
                raise ValueError(f"push() validation failed: {result['hard_issues']}")

        print(
            f"[Push] Syncing '{self.main_sheet_name}' -> {self.cfg_table_name} ..."
            + (" (force=True, validation skipped)" if force else "")
        )
        try:
            query = f"""
                CREATE OR REPLACE TABLE `{self.cfg_table_ref}` AS
                SELECT * FROM `{self.ext_table_ref}`
            """
            job = self.bq_client.query(query)
            job.result()  # wait for completion

            row_count = self.bq_client.get_table(self.cfg_table_ref).num_rows
            print(f"[Push] Done. '{self.cfg_table_name}' now matches the external table ({row_count} rows)")
            self._log_sync("push", "SUCCESS", detail=f"{row_count} rows" + (" (forced)" if force else ""))

        except Exception as e:
            print(f"[Push] Failed: {e}")
            self._log_sync("push", "FAILED", detail=str(e))
            raise

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    def check_tables_exist(self) -> Dict[str, bool]:
        """Read-only sanity check: do the external and internal cfg
        tables exist yet?"""
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        table_names = [tbl.table_id for tbl in self.bq_client.list_tables(dataset_ref)]

        result = {
            self.ext_table_name: self.ext_table_name in table_names,
            self.cfg_table_name: self.cfg_table_name in table_names,
        }
        for name, exists in result.items():
            status = "exists" if exists else "does NOT exist"
            print(f"Table '{name}' {status} in dataset '{self.dataset_id}'.")
        return result

"""config_sheet_syncer

Shared class for syncing a Google Sheet <-> BigQuery config table pair.

Used by multiple Colab notebooks (e.g. the source-config sync notebook
and the indicator/stage-config notebook) — each notebook does its own
auth/client bootstrapping and env config, then instantiates
`ConfigSheetSyncer` with the constructor arguments that fit its use case:

    - tmp_sheet_name: leave as None for a "direct edit on main" workflow
      (e.g. source config); pass a name to enable write_to_tmp_sheet() /
      promote_tmp_to_main() as a human-review buffer (e.g. stage config).
    - row_key_column: which column uniquely identifies a row, used when
      comparing Sheet vs. cfg_table rows in validate(). Defaults to
      "code"; pass a different column (e.g. "afs_id") if that's not the
      right key for a given sheet.
    - process_fn: an optional (df) -> df hook accepted by
      get_cfg_dataframe() / pull() / push(), for any use-case-specific
      DataFrame processing. Keep that logic in the calling notebook, not
      in this module — this class is meant to stay generic across configs.

See the class and method docstrings below for full details.
"""

from datetime import datetime
from typing import Callable, Dict, Optional

import gspread
import pandas as pd
from google.cloud import bigquery


class ConfigSheetSyncer:
    """Manages the data flow between a Google Sheet (human-edited config)
    and a BigQuery config table, including pre-sync validation and a
    sync log.

    This is the SAME class used for both the "source" config sync and the
    "stage" (indicator) config sync — the two use cases differ only in the
    constructor arguments:
        - tmp_sheet_name: source config doesn't use a tmp sheet (leave as
          None, the default); stage config passes one, used as a
          human-review buffer before overwriting main.
        - row_key_column: which column uniquely identifies a row, used
          when comparing Sheet vs. cfg_table row-by-row (source uses
          "code"; stage uses "afs_id").

    Each instance is bound to:
        - one worksheet (where a human edits config)
        - one log worksheet (records this instance's pull/push/promote history)
        - one BigQuery native table (the landed config table)
        - optionally, one tmp worksheet (a staging area for review before
          overwriting main)

    Create multiple instances to manage different configs (source cfg,
    stage/indicator cfg, ...) independently.
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
        tmp_sheet_name: Optional[str] = None,
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
            tmp_sheet_name: optional tmp worksheet name. If provided, this
                instance supports write_to_tmp_sheet() / promote_tmp_to_main()
                as a human-review buffer before overwriting main. If not
                provided (default None), those two methods raise — this
                instance is treated as having no tmp sheet, matching the
                simpler source-config workflow that edits main directly.
            row_key_column: the column used as a unique row key when
                comparing Sheet vs. cfg_table rows in validate(). Defaults
                to "code" (source config); pass something like "afs_id"
                for configs that use a different key.
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
        self.tmp_sheet_name = tmp_sheet_name

        # 1. Main worksheet handle. Intentionally NOT auto-created: if the
        #    name is wrong or the sheet doesn't exist, we want a loud error
        #    instead of silently creating an empty sheet that could be
        #    mistaken for real config data.
        self.main_ws = self.sh.worksheet(self.main_sheet_name)

        # 2. Optional tmp worksheet — a scratch/review area, so it's fine
        #    to auto-create it if missing.
        self.tmp_ws = (
            self._get_or_create_worksheet(self.tmp_sheet_name)
            if self.tmp_sheet_name
            else None
        )

        # 3. Log worksheet handle.
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

    def _get_or_create_worksheet(self, name: str, rows: int = 1000, cols: int = 26) -> gspread.Worksheet:
        """Get a worksheet by name, creating an empty one if it doesn't exist.

        Used for the tmp worksheet (a scratch area, safe to auto-create).
        main_ws deliberately does NOT go through this helper — see the
        comment in __init__.
        """
        try:
            return self.sh.worksheet(name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{name}' not found. Creating an empty one.")
            return self.sh.add_worksheet(title=name, rows=rows, cols=cols)

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
                "direction",     # "pull" (tbl->Sheet), "push" (Sheet->tbl), or "promote" (tmp->main)
                "sheet_name",
                "cfg_table",
                "status",        # SUCCESS / FAILED / SKIPPED
                "detail",        # row count, error message, etc.
            ]
            log_ws.append_row(headers)
            return log_ws

    def _log_sync(self, direction: str, status: str, detail: str = ""):
        """Record one pull / push / promote log entry."""
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
    def get_cfg_dataframe(
        self,
        process_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        raw_strings: bool = True,
    ) -> pd.DataFrame:
        """Read the main worksheet into a DataFrame.

        Args:
            process_fn: optional custom processing hook, signature
                (df: pd.DataFrame) -> pd.DataFrame. Not passed (default
                None) means no processing — the DataFrame is returned as
                read. Pass a different function each time you need
                different processing; this class stays generic.
            raw_strings: if True (default), read every cell as a plain
                string via get_all_values() (header = first row). This
                avoids gspread's get_all_records(), which auto-casts
                numeric-looking cells to int/float and would silently turn
                a value like "007" into the integer 7, dropping the
                leading zeros. Set raw_strings=False to use
                get_all_records() instead and let gspread infer types.

        Returns:
            The (optionally processed) DataFrame.
        """
        if raw_strings:
            values = self.main_ws.get_all_values()
            df = pd.DataFrame(values[1:], columns=values[0]) if values else pd.DataFrame()
        else:
            records = self.main_ws.get_all_records()
            df = pd.DataFrame(records)

        if process_fn is not None:
            df = process_fn(df)
        return df

    def get_cfg_dataframe_from_bq(self) -> pd.DataFrame:
        """Query the full cfg_table (BQ) into a DataFrame, with no processing."""
        return self.bq_client.query(f"SELECT * FROM `{self.cfg_table_ref}`").result().to_dataframe()

    # ------------------------------------------------------------------
    # tmp sheet: optional human-review buffer before overwriting main
    # ------------------------------------------------------------------
    def write_to_tmp_sheet(self, df: pd.DataFrame):
        """Full overwrite of the tmp worksheet with the given DataFrame.

        Requires this instance to have been constructed with
        tmp_sheet_name set; raises otherwise (e.g. the source-config
        syncer, which edits main directly and has no tmp sheet).
        """
        if self.tmp_ws is None:
            raise ValueError(
                "This ConfigSheetSyncer instance has no tmp sheet configured "
                "(tmp_sheet_name was not passed to __init__)."
            )
        output_data = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
        self.tmp_ws.clear()
        self.tmp_ws.update(output_data)
        print(f"Wrote {len(df)} rows to tmp sheet '{self.tmp_sheet_name}'.")

    def promote_tmp_to_main(self, confirm: bool = False):
        """Full overwrite of main with tmp's current content.

        Uses `confirm`, not `force`: this is a plain on/off gate for a
        destructive action that has no validation step to skip (unlike
        pull()/push(), which use `force` to skip an existing validation
        step). Nothing happens unless confirm=True is passed explicitly,
        after a human has reviewed the tmp sheet.

        Requires this instance to have been constructed with
        tmp_sheet_name set.
        """
        if self.tmp_ws is None:
            raise ValueError(
                "This ConfigSheetSyncer instance has no tmp sheet configured "
                "(tmp_sheet_name was not passed to __init__)."
            )
        if not confirm:
            print(
                "promote_tmp_to_main skipped (confirm=False). "
                "Review the tmp sheet, then call again with confirm=True."
            )
            return

        tmp_data = self.tmp_ws.get_all_values()
        self.main_ws.clear()
        self.main_ws.update(tmp_data)
        row_count = max(len(tmp_data) - 1, 0)
        print(f"Promoted '{self.tmp_sheet_name}' -> '{self.main_sheet_name}' ({row_count} rows).")
        self._log_sync("promote", "SUCCESS", detail=f"{row_count} rows")

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

    def validate(
        self,
        process_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    ) -> dict:
        """Standalone pre-sync validation — can be called any time on its
        own. Performs no sync action.

        Checks:
            - hard_issues: is Sheet / cfg_table empty, blank/duplicate column names
            - notes: Sheet vs. cfg_table column structure and row-level differences

        Args:
            process_fn: optional custom processing hook, signature (df) -> df,
                applied to sheet_df right after reading, before validation
                and comparison. Not passed (default None) means no processing.

        Returns:
            {
                "sheet_df": DataFrame,
                "bq_df": DataFrame,
                "hard_issues": list[str],   # non-empty means there's a hard problem
                "notes": list[str],         # informational only, doesn't block syncing
            }
        """
        print("[Validate] Starting validation ...")

        sheet_df = self.get_cfg_dataframe(process_fn=process_fn)
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
    def pull(
        self,
        force: bool = False,
        process_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    ):
        """cfg_table -> Sheet (read everything, overwrite the worksheet).

        Workflow stage: before a human starts editing the Sheet, pull down
        the latest state from cfg_table (e.g. after a pipeline run updated
        it), so the Sheet reflects the current state.

        Args:
            force: True skips validation entirely and syncs directly.
                False (default) calls validate() first, and aborts if
                hard_issues are found.
            process_fn: optional custom processing hook, signature
                (df) -> df, only relevant when force=False (it's forwarded
                into the internal validate() call, applied to the sheet_df
                read there). Not passed (default None) means no processing.
        """
        if force:
            bq_df = self.get_cfg_dataframe_from_bq()
        else:
            result = self.validate(process_fn=process_fn)
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
            df = df.applymap(lambda v: str(v) if not isinstance(v, (str, int, float, bool)) else v)

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
    def push(
        self,
        force: bool = False,
        process_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    ):
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

        Note: the data push() actually writes to BQ comes from the
        external table (a live pointer to the Sheet), not from any
        process_fn-processed DataFrame — process_fn here only affects the
        sheet_df used for validation/comparison inside push(). If you
        need the processed result to actually land in cfg_table, edit
        the Sheet itself first (e.g. via write_to_tmp_sheet +
        promote_tmp_to_main), then push.

        Workflow stage: after a human finishes editing the Sheet, push
        it to cfg_table.

        Args:
            force: True skips validation entirely and syncs directly.
                False (default) calls validate() first, and aborts if
                hard_issues are found.
            process_fn: optional custom processing hook, signature
                (df) -> df, only relevant when force=False (forwarded
                into the internal validate() call). Not passed (default
                None) means no processing.
        """
        if not force:
            result = self.validate(process_fn=process_fn)
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

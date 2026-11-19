# -*- coding: utf-8 -*-
"""view_manager.py

ViewManager — builds the view `src_{afs_source_code}_vw` from the external
table `src_{afs_source_code}_exttbl` by rendering the source's SQL
template `{afs_source_code}.sql`. The external table and the view live in
the same dataset (`dataset_id`).

Workflow for one source:
    1. Resolve names: external table, template file, target view
    2. Check that the external table exists; raise if it does not
    3. Check that the template exists; raise if it does not
    4. Render the template (fill the placeholders)
    5. Optionally confirm (warns when an existing view will be overwritten)
    6. Execute CREATE OR REPLACE VIEW

Template placeholders: {{ target_view }}, {{ source_table }}, {{ ref_m49_table }}

Environment-free: the dataset, the m49 reference table and the template
folder are passed in by the caller. Table ids are always full ids
(`project.dataset.table`), dataset ids are `project.dataset`.

`confirm=True` (the default) asks on the console before writing, for
interactive use; pass `confirm=False` to run unattended.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Union

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


# afs_source_code is used inside table names: lowercase letters, digits and underscores only.
_CODE_RE = re.compile(r"^[a-z0-9_]+$")
_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")

# Naming rules — change them here only.
SOURCE_TABLE_PATTERN = "src_{code}_exttbl"
TARGET_VIEW_PATTERN = "src_{code}_vw"
TEMPLATE_PATTERN = "{code}.sql"

# Columns of the DataFrame returned by run_batch() (and of each run() result).
RESULT_COLUMNS = ["afs_source_code", "target_view", "action", "status", "message"]


def _full_id(value: str, n_parts: int, label: str) -> str:
    """Strip backticks/whitespace and check that `value` is a full id:
    `project.dataset` (n_parts=2) or `project.dataset.table` (n_parts=3)."""
    value = (value or "").strip().strip("`")
    if len(value.split(".")) < n_parts:
        shape = "project.dataset" if n_parts == 2 else "project.dataset.table"
        raise ValueError(f"{label} must be a full id ({shape}), got {value!r}")
    return value


class ViewManager:
    """Renders the per-source SQL templates into `src_*_vw` views.

    Usage:
        vm = ViewManager(
            bq_client=bq_client,
            dataset_id="my-project.my_dataset",
            ref_m49_table_id="my-project.my_dataset.dim_ref_scope_m49_tbl",
            template_dir="/path/to/src2stg_view_templates",
        )
        vm.run("faostat_qcl")                         # one source, asks first
        vm.run_batch(None, confirm=False)            # every template, unattended
    """

    def __init__(
        self,
        bq_client: bigquery.Client,
        dataset_id: str,
        ref_m49_table_id: str,
        template_dir: str,
    ):
        """
        dataset_id: full dataset id (`project.dataset`) holding both the
            `src_*_exttbl` external tables and the `src_*_vw` views.
        ref_m49_table_id: full table id of the m49 reference table.
        template_dir: folder holding the `{afs_source_code}.sql` templates.
        """
        self.bq_client = bq_client
        self.dataset_id = _full_id(dataset_id, 2, "dataset_id")
        self.ref_m49_table_id = _full_id(ref_m49_table_id, 3, "ref_m49_table_id")
        self.template_dir = Path(template_dir)
        if not self.template_dir.is_dir():
            raise FileNotFoundError(f"Template directory not found: {self.template_dir}")

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------

    def list_source_codes(self) -> List[str]:
        """afs_source_code of every template in the template folder."""
        return sorted(p.stem for p in self.template_dir.glob("*.sql"))

    def _resolve(self, code: str) -> Dict[str, Any]:
        """External table, template path and target view of a code."""
        if not _CODE_RE.match(code or ""):
            raise ValueError(
                f"afs_source_code may only contain lowercase letters, digits and underscores: {code!r}"
            )
        return {
            "source_table": f"{self.dataset_id}.{SOURCE_TABLE_PATTERN.format(code=code)}",
            "target_view": f"{self.dataset_id}.{TARGET_VIEW_PATTERN.format(code=code)}",
            "ref_m49_table": self.ref_m49_table_id,
            "template_path": self.template_dir / TEMPLATE_PATTERN.format(code=code),
        }

    def _exists(self, table_id: str) -> bool:
        try:
            self.bq_client.get_table(table_id)
            return True
        except NotFound:
            return False

    def _check(self, code: str) -> Dict[str, Any]:
        """Resolve a code and raise if its external table or template is
        missing. Returns the resolved names plus whether the view exists."""
        names = self._resolve(code)
        if not self._exists(names["source_table"]):
            raise LookupError(f"External table not found: {names['source_table']}")
        if not names["template_path"].is_file():
            raise FileNotFoundError(f"Template not found: {names['template_path']}")
        names["view_exists"] = self._exists(names["target_view"])
        return names

    def _render(self, code: str) -> str:
        """Read the code's template file and return the filled SQL."""
        text = self._resolve(code)["template_path"].read_text(encoding="utf-8")
        return self.render_sql(text, code)

    def _execute(self, sql: str) -> None:
        self.bq_client.query(sql).result()

    # ------------------------------------------------------------------
    # Public entry point — one source
    # ------------------------------------------------------------------

    def run(self, afs_source_code: str, confirm: bool = True) -> Dict[str, Any]:
        """Create or replace one view. Raises if the external table or the
        template does not exist. With confirm=True, asks first (type `y`).
        Returns one result row (see RESULT_COLUMNS)."""
        names = self._check(afs_source_code)
        action = "REPLACE" if names["view_exists"] else "CREATE"
        result = {"afs_source_code": afs_source_code, "target_view": names["target_view"],
                  "action": action, "status": "SKIPPED", "message": ""}

        if confirm:
            prompt = (f"WARNING: view already exists and will be overwritten: {names['target_view']}\nOverwrite? [y/N] "
                      if names["view_exists"]
                      else f"A new view will be created: {names['target_view']}\nCreate? [y/N] ")
            if input(prompt).strip().lower() != "y":
                result["message"] = "Cancelled"
                print("Cancelled.")
                return result

        self._execute(self._render(afs_source_code))
        result["status"] = "SUCCESS"
        print(f"{action}: {names['target_view']}")
        return result

    # ------------------------------------------------------------------
    # Public entry point — many sources
    # ------------------------------------------------------------------

    def run_batch(
        self,
        afs_source_codes: Union[None, str, Iterable[str]] = None,
        confirm: bool = True,
    ) -> pd.DataFrame:
        """Create or replace many views. afs_source_codes=None takes every
        template in the template folder.

        Every code is checked first and the plan is printed (create /
        replace / skip). With confirm=True one answer covers the batch:
        `a` runs all, `n` creates new views only, anything else cancels.
        With confirm=False everything is created or replaced. Codes whose
        external table or template is missing are SKIPPED; one code failing
        doesn't stop the others. Returns one row per code.
        """
        if afs_source_codes is None:
            codes = self.list_source_codes()
        elif isinstance(afs_source_codes, str):
            codes = [afs_source_codes]
        else:
            codes = list(dict.fromkeys(afs_source_codes))

        # 1. Check and classify
        rows: List[Dict[str, Any]] = []
        for code in codes:
            row = {"afs_source_code": code, "target_view": "", "action": "", "status": "", "message": ""}
            try:
                names = self._check(code)
                row["target_view"] = names["target_view"]
                row["action"] = "REPLACE" if names["view_exists"] else "CREATE"
            except Exception as exc:  # noqa: BLE001 - a bad code is skipped, not fatal
                row["status"], row["message"] = "SKIPPED", f"{type(exc).__name__}: {exc}"
            rows.append(row)

        new = [r["afs_source_code"] for r in rows if r["action"] == "CREATE"]
        replace = [r["afs_source_code"] for r in rows if r["action"] == "REPLACE"]
        skipped = [r for r in rows if r["status"] == "SKIPPED"]
        print(f"{len(codes)} source(s): create {len(new)}, replace {len(replace)}, skip {len(skipped)}")
        if new:
            print(f"  Create:  {new}")
        if replace:
            print(f"  Replace: {replace}")
        for r in skipped:
            print(f"  Skip:    {r['afs_source_code']}: {r['message']}")

        # 2. Confirm
        allowed = {"CREATE", "REPLACE"}
        if confirm and (new or replace):
            answer = input(
                "\n[a] Run all (create + replace)  [n] Create new only  [other] Cancel\nYour choice: "
            ).strip().lower()
            if answer == "n":
                allowed = {"CREATE"}
            elif answer != "a":
                allowed = set()
                print("Cancelled.")

        # 3. Execute
        for row in rows:
            if row["status"]:
                continue
            if row["action"] not in allowed:
                row["status"], row["message"] = "SKIPPED", "Not selected"
                continue
            try:
                self._execute(self._render(row["afs_source_code"]))
                row["status"] = "SUCCESS"
            except Exception as exc:  # noqa: BLE001 - one view failing doesn't stop the others
                row["status"], row["message"] = "FAILED", f"{type(exc).__name__}: {exc}"
            print(f"{row['afs_source_code']} -> {row['action']} {row['status']}"
                  + (f": {row['message']}" if row["status"] == "FAILED" else ""))

        df = pd.DataFrame(rows, columns=RESULT_COLUMNS)
        counts = df["status"].value_counts().to_dict()
        print(f"\nBatch finished: {len(df)} source(s) — "
              + ", ".join(f"{s}: {counts.get(s, 0)}" for s in ("SUCCESS", "SKIPPED", "FAILED")))
        return df

    # ------------------------------------------------------------------
    # Render a template string (for manual testing in the BigQuery console)
    # ------------------------------------------------------------------

    def render_sql(self, template: str, afs_source_code: str) -> str:
        """Fill the placeholders of a SQL template string for the given
        afs_source_code and return the full SQL (including CREATE OR
        REPLACE VIEW). Pure string processing: no file is read and
        BigQuery is not contacted."""
        names = self._resolve(afs_source_code)
        values = {
            "target_view": f"`{names['target_view']}`",
            "source_table": f"`{names['source_table']}`",
            "ref_m49_table": f"`{names['ref_m49_table']}`",
        }

        def _sub(m: re.Match) -> str:
            key = m.group(1)
            if key not in values:
                raise KeyError(f"Unknown placeholder in template: {{{{ {key} }}}}")
            return values[key]

        return _PLACEHOLDER_RE.sub(_sub, template)

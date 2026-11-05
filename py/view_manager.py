"""
ViewManager: build the view src_{code}_vw from the external table src_{code}_exttbl
for a given afs_source_code. The external table and the view live in the same
dataset (src_dataset). The m49 reference table is passed in as a full table id.

Workflow (single code):
    1. Resolve names: external table, template, target view
    2. Check that the external table exists; raise if it does not
    3. Check that the template exists; raise if it does not
    4. Render the template (replace the three placeholders)
    5. Confirm: if the view already exists, warn that it will be overwritten;
       nothing is executed unless you type y
    6. Execute CREATE OR REPLACE VIEW

Template placeholders: {{ target_view }}, {{ source_table }}, {{ ref_m49_table }}
This file contains no environment info or credentials; all environment
parameters are passed in from the notebook.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# afs_source_code is used inside table names: only lowercase letters, digits and underscores
_CODE_RE = re.compile(r"^[a-z0-9_]+$")
_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


class ViewManager:
    # Naming rules: change them here only
    SOURCE_TABLE_PATTERN = "src_{code}_exttbl"
    TARGET_VIEW_PATTERN = "src_{code}_vw"
    TEMPLATE_PATTERN = "{code}.sql"

    def __init__(
        self,
        client: bigquery.Client,
        project: str,
        src_dataset: str,
        ref_m49_table: str,
        template_dir: str = "templates",
    ):
        """
        ref_m49_table: full table id of the m49 reference table,
                       e.g. "my-project.my_dataset.dim_ref_scope_m49_tbl".
        """
        ref_m49_table = (ref_m49_table or "").strip().strip("`")
        if not ref_m49_table:
            raise ValueError("ref_m49_table must be a full table id: project.dataset.table")

        self.client = client
        self.project = project
        self.src_dataset = src_dataset
        self.ref_m49_table = ref_m49_table
        self.template_dir = Path(template_dir)

        if not self.template_dir.is_dir():
            raise FileNotFoundError(f"Template directory not found: {self.template_dir}")

    # =======================================================================
    # Run
    # =======================================================================
    def run(self, code: str, confirm: bool = True) -> str:
        """
        Single run. Returns 'created' / 'replaced' / 'skipped'.
        Raises if the external table or the template does not exist.
        """
        names = self._resolve(code)
        self._check_source(code)
        self._check_template(code)
        sql = self._render(code)
        exists = self._view_exists(code)

        if confirm:
            if exists:
                msg = (f"⚠️ View already exists and will be overwritten: {names['target_view']}\n"
                       f"Overwrite? [y/N] ")
            else:
                msg = f"A new view will be created: {names['target_view']}\nCreate? [y/N] "
            if input(msg).strip().lower() != "y":
                print("Cancelled")
                return "skipped"

        self._execute(sql)
        status = "replaced" if exists else "created"
        print(f"{'Replaced' if exists else 'Created'}: {names['target_view']}")
        return status

    def run_all(self, codes: Optional[list[str]] = None, confirm: bool = True) -> dict:
        """
        Batch run. Checks every code first and prints the plan, asks for confirmation
        once, then executes the codes one by one.
        Codes whose external table or template is missing are skipped; a failure on
        one code does not stop the others.
        """
        codes = codes if codes is not None else self.list_codes()

        # ---- 1. Check and classify ----
        plan = {"new": [], "replace": [], "error": {}}
        for code in codes:
            try:
                self._resolve(code)
                self._check_source(code)
                self._check_template(code)
                plan["replace" if self._view_exists(code) else "new"].append(code)
            except Exception as e:  # noqa: BLE001
                plan["error"][code] = f"{type(e).__name__}: {e}"

        print(f"{len(codes)} code(s) in total")
        print(f"  Create {len(plan['new'])}: {plan['new']}")
        print(f"  Overwrite {len(plan['replace'])} (view already exists): {plan['replace']}")
        if plan["error"]:
            print(f"  Skip {len(plan['error'])} (checks failed):")
            for code, err in plan["error"].items():
                print(f"    - {code}: {err}")

        # ---- 2. Confirm ----
        todo = plan["new"] + plan["replace"]
        if confirm and todo:
            ans = input(
                "\n[a] Run all (create + overwrite)  [n] Create new only, no overwrite  "
                "[other] Cancel\nYour choice: "
            ).strip().lower()
            if ans == "n":
                todo = plan["new"]
            elif ans != "a":
                print("Cancelled")
                return {"created": [], "replaced": [], "failed": {}, "skipped": plan["error"]}

        # ---- 3. Execute ----
        result = {"created": [], "replaced": [], "failed": {}, "skipped": dict(plan["error"])}
        for code in plan["new"] + plan["replace"]:
            if code not in todo:
                result["skipped"][code] = "Overwrite not selected"
                continue
            try:
                self._execute(self._render(code))
                key = "replaced" if code in plan["replace"] else "created"
                result[key].append(code)
                print(f"OK    {code}")
            except Exception as e:  # noqa: BLE001
                result["failed"][code] = f"{type(e).__name__}: {e}"
                print(f"FAIL  {code}: {result['failed'][code]}")

        print(
            f"\n===== Summary: created {len(result['created'])}, "
            f"replaced {len(result['replaced'])}, failed {len(result['failed'])}, "
            f"skipped {len(result['skipped'])} ====="
        )
        return result

    def list_codes(self) -> list[str]:
        """Codes of all templates in the template directory."""
        return sorted(p.stem for p in self.template_dir.glob("*.sql"))

    # =======================================================================
    # Render a template string (for manual testing in the BigQuery console)
    # =======================================================================
    def render_sql(self, template: str, code: str) -> str:
        """
        Fill the placeholders of a given SQL template string for the given
        afs_source_code and return the full SQL (including CREATE OR REPLACE VIEW).
        Pure string processing: no file is read and BigQuery is not contacted.
        """
        names = self._resolve(code)
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

    # =======================================================================
    # Internal
    # =======================================================================
    def _resolve(self, code: str) -> dict:
        """Resolve the external table, template path and target view for a code."""
        if not _CODE_RE.match(code or ""):
            raise ValueError(
                f"afs_source_code may only contain lowercase letters, digits and underscores: {code!r}"
            )
        return {
            "source_table": f"{self.project}.{self.src_dataset}."
                            + self.SOURCE_TABLE_PATTERN.format(code=code),
            "target_view": f"{self.project}.{self.src_dataset}."
                           + self.TARGET_VIEW_PATTERN.format(code=code),
            "ref_m49_table": self.ref_m49_table,
            "template_path": self.template_dir / self.TEMPLATE_PATTERN.format(code=code),
        }

    def _check_source(self, code: str) -> None:
        """Raise if the external table does not exist."""
        table_id = self._resolve(code)["source_table"]
        try:
            self.client.get_table(table_id)
        except NotFound:
            raise LookupError(f"External table not found: {table_id}") from None

    def _check_template(self, code: str) -> Path:
        """Raise if the template file does not exist; return its path."""
        path = self._resolve(code)["template_path"]
        if not path.is_file():
            raise FileNotFoundError(f"Template not found: {path}")
        return path

    def _render(self, code: str) -> str:
        """Read the template file of a code and return the filled SQL."""
        text = self._check_template(code).read_text(encoding="utf-8")
        return self.render_sql(text, code)

    def _view_exists(self, code: str) -> bool:
        """Whether the target view already exists."""
        try:
            self.client.get_table(self._resolve(code)["target_view"])
            return True
        except NotFound:
            return False

    def _execute(self, sql: str) -> None:
        self.client.query(sql).result()

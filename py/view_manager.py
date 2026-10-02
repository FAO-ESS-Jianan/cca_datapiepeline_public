"""
ViewManager：根据 afs_source_code 把外部表 src_{code}_exttbl 构建成 view src_{code}_vw。
外部表、view 和 m49 参照表默认都在同一个 dataset（src_dataset）。

运行逻辑（单个 code）：
    1. 推导地址：外部表、模板、目标 view
    2. 检查外部表是否存在，不存在则报错
    3. 检查模板是否存在，不存在则报错
    4. 渲染模板（替换三个占位符）
    5. 确认：view 已存在时提示将被覆盖，需要输入 y 才会执行
    6. 执行 CREATE OR REPLACE VIEW

模板占位符：{{ target_view }}、{{ source_table }}、{{ ref_m49_table }}
本文件不包含任何环境信息或凭证，所有环境参数由 notebook 传入。
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# afs_source_code 会被拼进表名，只允许小写字母、数字和下划线
_CODE_RE = re.compile(r"^[a-z0-9_]+$")
_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


class ViewManager:
    # 命名规则：如需修改，只改这里
    SOURCE_TABLE_PATTERN = "src_{code}_exttbl"
    TARGET_VIEW_PATTERN = "src_{code}_vw"
    TEMPLATE_PATTERN = "{code}.sql"

    def __init__(
        self,
        client: bigquery.Client,
        project: str,
        src_dataset: str,
        ref_dataset: Optional[str] = None,
        ref_table: str = "dim_ref_scope_m49_tbl",
        template_dir: str = "templates",
    ):
        self.client = client
        self.project = project
        self.src_dataset = src_dataset
        self.ref_table = f"{project}.{ref_dataset or src_dataset}.{ref_table}"
        self.template_dir = Path(template_dir)

        if not self.template_dir.is_dir():
            raise FileNotFoundError(f"模板目录不存在: {self.template_dir}")

    # =======================================================================
    # 运行
    # =======================================================================
    def run(self, code: str, confirm: bool = True) -> str:
        """单个运行。返回 'created' / 'replaced' / 'skipped'；外部表或模板不存在时报错。"""
        names = self.resolve(code)
        self.check_source(code)
        self.check_template(code)
        sql = self.render(code)
        exists = self.view_exists(code)

        if confirm:
            if exists:
                msg = f"⚠️ view 已存在，将被覆盖：{names['target_view']}\n确认覆盖？[y/N] "
            else:
                msg = f"将创建新 view：{names['target_view']}\n确认创建？[y/N] "
            if input(msg).strip().lower() != "y":
                print("已取消")
                return "skipped"

        self._execute(sql)
        status = "replaced" if exists else "created"
        print(f"{'覆盖' if exists else '创建'}完成：{names['target_view']}")
        return status

    def run_all(self, codes: Optional[list[str]] = None, confirm: bool = True) -> dict:
        """
        批量运行。先检查所有 code 并列出计划，确认一次后逐个执行。
        外部表或模板缺失的 code 会被跳过，单个执行失败不会中断其他 code。
        """
        codes = codes if codes is not None else self.list_codes()

        # ---- 1. 检查并分类 ----
        plan = {"new": [], "replace": [], "error": {}}
        for code in codes:
            try:
                self.resolve(code)
                self.check_source(code)
                self.check_template(code)
                plan["replace" if self.view_exists(code) else "new"].append(code)
            except Exception as e:  # noqa: BLE001
                plan["error"][code] = f"{type(e).__name__}: {e}"

        print(f"共 {len(codes)} 个 code")
        print(f"  新建 {len(plan['new'])} 个：{plan['new']}")
        print(f"  覆盖 {len(plan['replace'])} 个（view 已存在）：{plan['replace']}")
        if plan["error"]:
            print(f"  跳过 {len(plan['error'])} 个（检查未通过）：")
            for code, err in plan["error"].items():
                print(f"    - {code}: {err}")

        # ---- 2. 确认 ----
        todo = plan["new"] + plan["replace"]
        if confirm and todo:
            ans = input(
                "\n[a] 全部执行（新建 + 覆盖）  [n] 只新建，不覆盖  [其他] 取消\n请选择："
            ).strip().lower()
            if ans == "n":
                todo = plan["new"]
            elif ans != "a":
                print("已取消")
                return {"created": [], "replaced": [], "failed": {}, "skipped": plan["error"]}

        # ---- 3. 执行 ----
        result = {"created": [], "replaced": [], "failed": {}, "skipped": dict(plan["error"])}
        for code in plan["new"] + plan["replace"]:
            if code not in todo:
                result["skipped"][code] = "未选择覆盖"
                continue
            try:
                self._execute(self.render(code))
                key = "replaced" if code in plan["replace"] else "created"
                result[key].append(code)
                print(f"OK    {code}")
            except Exception as e:  # noqa: BLE001
                result["failed"][code] = f"{type(e).__name__}: {e}"
                print(f"FAIL  {code}: {result['failed'][code]}")

        print(
            f"\n===== 汇总：新建 {len(result['created'])}，覆盖 {len(result['replaced'])}，"
            f"失败 {len(result['failed'])}，跳过 {len(result['skipped'])} ====="
        )
        return result

    # =======================================================================
    # 单个步骤（run 内部使用，也可以在 notebook 里逐步调用来调试）
    # =======================================================================
    def list_codes(self) -> list[str]:
        """templates/ 下所有模板对应的 code。"""
        return sorted(p.stem for p in self.template_dir.glob("*.sql"))

    def resolve(self, code: str) -> dict:
        """推导外部表、模板、目标 view 的地址。"""
        if not _CODE_RE.match(code or ""):
            raise ValueError(f"afs_source_code 只允许小写字母、数字和下划线: {code!r}")
        return {
            "source_table": f"{self.project}.{self.src_dataset}."
                            + self.SOURCE_TABLE_PATTERN.format(code=code),
            "target_view": f"{self.project}.{self.src_dataset}."
                           + self.TARGET_VIEW_PATTERN.format(code=code),
            "ref_m49_table": self.ref_table,
            "template_path": self.template_dir / self.TEMPLATE_PATTERN.format(code=code),
        }

    def check_source(self, code: str) -> list[tuple[str, str]]:
        """检查外部表是否存在，返回它的列名和类型。"""
        table_id = self.resolve(code)["source_table"]
        try:
            table = self.client.get_table(table_id)
        except NotFound:
            raise LookupError(f"外部表不存在: {table_id}") from None
        return [(f.name, f.field_type) for f in table.schema]

    def check_template(self, code: str) -> Path:
        """检查模板文件是否存在。"""
        path = self.resolve(code)["template_path"]
        if not path.is_file():
            raise FileNotFoundError(f"模板不存在: {path}")
        return path

    def render(self, code: str) -> str:
        """读取模板并替换占位符，返回完整 SQL。"""
        names = self.resolve(code)
        text = self.check_template(code).read_text(encoding="utf-8")
        values = {
            "target_view": f"`{names['target_view']}`",
            "source_table": f"`{names['source_table']}`",
            "ref_m49_table": f"`{names['ref_m49_table']}`",
        }

        def _sub(m: re.Match) -> str:
            key = m.group(1)
            if key not in values:
                raise KeyError(f"模板中有未知占位符: {{{{ {key} }}}}")
            return values[key]

        return _PLACEHOLDER_RE.sub(_sub, text)

    def view_exists(self, code: str) -> bool:
        """目标 view 是否已存在。"""
        try:
            self.client.get_table(self.resolve(code)["target_view"])
            return True
        except NotFound:
            return False

    # =======================================================================
    # 调试辅助
    # =======================================================================
    def dry_run(self, code: str) -> list[tuple[str, str]]:
        """只检查 SELECT 部分能否运行，返回输出列名和类型。不建 view，不产生费用。"""
        query = self._select_part(self.render(code))
        cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = self.client.query(query, job_config=cfg)
        return [(f.name, f.field_type) for f in (job.schema or [])]

    def preview(self, code: str, limit: int = 20):
        """不建 view，直接查看前几行（会产生查询费用）。"""
        query = f"SELECT * FROM (\n{self._select_part(self.render(code))}\n) LIMIT {int(limit)}"
        return self.client.query(query).to_dataframe()

    def find_sources_without_template(self) -> list[str]:
        """源 dataset 里有外部表、但没有对应模板的 code。"""
        prefix, suffix = self.SOURCE_TABLE_PATTERN.split("{code}")
        have = set(self.list_codes())
        found = []
        for t in self.client.list_tables(f"{self.project}.{self.src_dataset}"):
            name = t.table_id
            if name.startswith(prefix) and name.endswith(suffix):
                code = name[len(prefix):len(name) - len(suffix)]
                if code not in have:
                    found.append(code)
        return sorted(found)

    # =======================================================================
    # 内部
    # =======================================================================
    def _execute(self, sql: str) -> None:
        self.client.query(sql).result()

    @staticmethod
    def _select_part(sql: str) -> str:
        """去掉开头的 CREATE OR REPLACE VIEW ... AS，只保留查询部分。"""
        m = re.search(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+`[^`]+`\s+AS\s+(.*)$",
                      sql, re.IGNORECASE | re.DOTALL)
        if not m:
            raise ValueError("模板必须包含 CREATE OR REPLACE VIEW `...` AS")
        return m.group(1).strip().rstrip(";")

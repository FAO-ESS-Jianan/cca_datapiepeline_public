"""
ViewManager：根据 afs_source_code 构建 src 外部表 -> stg view，并记录日志。

- 本文件不包含任何环境信息或凭证，所有环境参数由 Colab notebook 传入。
- 命名规则（由 afs_source_code 推导）：
    模板    : <repo_dir>/templates/{code}.sql
    外部表  : {project}.{src_dataset}.src_{code}_exttbl
    目标view: {project}.{stg_dataset}.stg_{code}_vw
- 模板占位符：{{ target_view }}、{{ source_table }}、{{ ref_m49_table }}
"""
from __future__ import annotations

import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# 标识符格式
# ---------------------------------------------------------------------------
_CODE_RE = re.compile(r"^[a-z0-9_]+$")
_PROJECT_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")
_HEADER_RE = re.compile(
    r"^\s*(?:--[^\n]*\n\s*)*"                      # 允许开头的注释行
    r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(`[^`]+`)\s+AS\s+(.*)$",
    re.IGNORECASE | re.DOTALL,
)

# ---------------------------------------------------------------------------
# stg view 的标准输出结构（名称、顺序、类型）
# ---------------------------------------------------------------------------
EXPECTED_SCHEMA = [
    ("afs_m49_code", "STRING"),
    ("afs_iso3c", "STRING"),
    ("area_ref", "STRING"),
    ("area_name", "STRING"),
    ("afs_year", "INTEGER"),
    ("afs_value", "FLOAT"),
    ("value", "STRING"),
    ("GEO", "JSON"),
    ("TIME_PERIOD", "JSON"),
    ("SERIES", "JSON"),
    ("DIMS", "JSON"),
    ("ATTRS", "JSON"),
    ("afs_source", "STRING"),
    ("afs_source_updated_at", "STRING"),
    ("afs_source_ingested_at", "TIMESTAMP"),
    ("afs_source_priority", "STRING"),
]
_TYPE_ALIASES = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "BOOL": "BOOLEAN"}

# ---------------------------------------------------------------------------
# 日志表结构
# ---------------------------------------------------------------------------
LOG_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("run_mode", "STRING"),          # single / batch
    bigquery.SchemaField("afs_source_code", "STRING"),
    bigquery.SchemaField("src_table", "STRING"),
    bigquery.SchemaField("target_view", "STRING"),
    bigquery.SchemaField("status", "STRING"),            # success / failed
    bigquery.SchemaField("failed_step", "STRING"),
    bigquery.SchemaField("error_message", "STRING"),
    bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("finished_at", "TIMESTAMP"),
    bigquery.SchemaField("executed_by", "STRING"),
    bigquery.SchemaField("repo_commit", "STRING"),
]


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ViewManager:
    """管理 stg view 的单个更新与批量更新。"""

    def __init__(
        self,
        client: bigquery.Client,
        project: str,
        src_dataset: str,
        stg_dataset: str,
        ref_dataset: Optional[str] = None,
        ref_table: str = "dim_ref_scope_m49_tbl",
        repo_dir: str = ".",
        template_subdir: str = "templates",
        log_table: str = "log_src2stg_view_tbl",
        location: Optional[str] = None,
    ):
        ref_dataset = ref_dataset or src_dataset

        if not _PROJECT_RE.match(project):
            raise ValueError(f"project 格式不合法: {project!r}")
        for label, name in [
            ("src_dataset", src_dataset),
            ("stg_dataset", stg_dataset),
            ("ref_dataset", ref_dataset),
            ("ref_table", ref_table),
            ("log_table", log_table),
        ]:
            if not _NAME_RE.match(name):
                raise ValueError(f"{label} 格式不合法: {name!r}")

        self.client = client
        self.project = project
        self.src_dataset = src_dataset
        self.stg_dataset = stg_dataset
        self.ref_table_fq = f"{project}.{ref_dataset}.{ref_table}"
        self.log_table_fq = f"{project}.{stg_dataset}.{log_table}"
        self.location = location

        self.repo_dir = Path(repo_dir)
        self.template_dir = self.repo_dir / template_subdir
        if not self.template_dir.is_dir():
            raise FileNotFoundError(f"模板目录不存在: {self.template_dir}")

        self.repo_commit = self._get_repo_commit()
        self._log_ready = False

    # =======================================================================
    # 公开方法
    # =======================================================================
    def update_view(self, afs_source_code: str) -> dict:
        """单个更新：构建一个 stg view。"""
        run_id = uuid.uuid4().hex
        return self._run_one(afs_source_code, run_id, "single")

    def update_all(self) -> list[dict]:
        """批量更新：对 templates/ 下所有模板逐个构建，单个失败不中断。"""
        run_id = uuid.uuid4().hex
        codes = self.list_template_codes()
        print(f"批量运行 run_id={run_id}，共 {len(codes)} 个模板\n")

        results = [self._run_one(code, run_id, "batch") for code in codes]
        missing = self.find_ext_tables_without_template(codes)
        self._print_summary(results, missing)
        return results

    def list_template_codes(self) -> list[str]:
        """templates/ 目录下所有模板对应的 afs_source_code。"""
        return sorted(p.stem for p in self.template_dir.glob("*.sql"))

    def find_ext_tables_without_template(self, codes: Optional[list[str]] = None) -> list[str]:
        """源 dataset 里存在外部表、但没有对应模板的 afs_source_code。"""
        codes = set(codes if codes is not None else self.list_template_codes())
        pattern = re.compile(r"^src_(.+)_exttbl$")
        found = []
        for t in self.client.list_tables(f"{self.project}.{self.src_dataset}"):
            m = pattern.match(t.table_id)
            if m and m.group(1) not in codes:
                found.append(m.group(1))
        return sorted(found)

    # =======================================================================
    # 分步调试（不写日志）
    # =======================================================================
    def debug_resolve(self, afs_source_code: str) -> dict:
        """第 1 步：校验代码格式，返回推导出的各个地址。"""
        if not _CODE_RE.match(afs_source_code or ""):
            raise ValueError(f"afs_source_code 只允许小写字母、数字和下划线: {afs_source_code!r}")
        return self._resolve(afs_source_code)

    def debug_check_ext_table(self, afs_source_code: str) -> list[tuple[str, str]]:
        """第 2 步：检查外部表是否存在，并返回它的列名和类型，方便对照模板字段。"""
        names = self.debug_resolve(afs_source_code)
        self._check_ext_table(names["source_table"])
        table = self.client.get_table(names["source_table"])
        return [(f.name, f.field_type) for f in table.schema]

    def debug_render(self, afs_source_code: str) -> str:
        """第 3 步：渲染模板，返回完整 SQL。"""
        names = self.debug_resolve(afs_source_code)
        return self._render(names["template_path"], names)

    def debug_dry_run(self, afs_source_code: str) -> list[tuple[str, str]]:
        """第 4 步：dry run（不产生费用），返回输出结构。"""
        names = self.debug_resolve(afs_source_code)
        sql = self._render(names["template_path"], names)
        schema = self._dry_run(self._extract_body(sql, names["target_view"]))
        return [(f.name, f.field_type) for f in schema]

    def debug_check_schema(self, afs_source_code: str) -> None:
        """第 5 步：dry run 并比对 16 列标准结构，不符时抛出异常并列出差异。"""
        names = self.debug_resolve(afs_source_code)
        sql = self._render(names["template_path"], names)
        self._check_schema(self._dry_run(self._extract_body(sql, names["target_view"])))
        print("输出结构与标准一致")

    def debug_preview(self, afs_source_code: str, limit: int = 20):
        """在不建 view 的情况下预览结果（会产生查询费用，外部表通常会被整表扫描）。"""
        names = self.debug_resolve(afs_source_code)
        sql = self._render(names["template_path"], names)
        body = self._extract_body(sql, names["target_view"])
        query = f"SELECT * FROM (\n{body}\n) LIMIT {int(limit)}"
        return self.client.query(query, location=self.location).to_dataframe()

    def show_log(self, limit: int = 50, afs_source_code: Optional[str] = None):
        """查看最近的日志记录。"""
        where, params = "", []
        if afs_source_code:
            where = "WHERE afs_source_code = @code"
            params.append(bigquery.ScalarQueryParameter("code", "STRING", afs_source_code))
        params.append(bigquery.ScalarQueryParameter("lim", "INT64", int(limit)))
        sql = f"""
            SELECT * FROM `{self.log_table_fq}`
            {where}
            ORDER BY started_at DESC
            LIMIT @lim
        """
        cfg = bigquery.QueryJobConfig(query_parameters=params)
        return self.client.query(sql, job_config=cfg, location=self.location).to_dataframe()

    # =======================================================================
    # 单源流程
    # =======================================================================
    def _run_one(self, code: str, run_id: str, run_mode: str) -> dict:
        rec = {
            "run_id": run_id,
            "run_mode": run_mode,
            "afs_source_code": code,
            "src_table": None,
            "target_view": None,
            "status": None,
            "failed_step": None,
            "error_message": None,
            "started_at": _now(),
            "finished_at": None,
        }
        step = "validate_code"
        try:
            if not _CODE_RE.match(code or ""):
                raise ValueError(f"afs_source_code 只允许小写字母、数字和下划线: {code!r}")
            names = self._resolve(code)
            rec["src_table"] = names["source_table"]
            rec["target_view"] = names["target_view"]

            step = "check_ext_table"
            self._check_ext_table(names["source_table"])

            step = "render"
            sql = self._render(names["template_path"], names)

            step = "dry_run"
            body = self._extract_body(sql, names["target_view"])
            schema = self._dry_run(body)

            step = "check_schema"
            self._check_schema(schema)

            step = "execute"
            self.client.query(sql, location=self.location).result()

            rec["status"] = "success"
        except Exception as e:  # noqa: BLE001 —— 记录任何失败并继续
            rec["status"] = "failed"
            rec["failed_step"] = step
            rec["error_message"] = f"{type(e).__name__}: {e}"

        rec["finished_at"] = _now()

        try:
            self._write_log(rec)
        except Exception as e:  # noqa: BLE001
            print(f"  [警告] 日志写入失败: {type(e).__name__}: {e}")

        mark = "OK  " if rec["status"] == "success" else "FAIL"
        extra = "" if rec["status"] == "success" else f"  [{rec['failed_step']}] {rec['error_message']}"
        print(f"{mark} {code}{extra}")
        return rec

    def _resolve(self, code: str) -> dict:
        return {
            "source_table": f"{self.project}.{self.src_dataset}.src_{code}_exttbl",
            "target_view": f"{self.project}.{self.stg_dataset}.stg_{code}_vw",
            "ref_m49_table": self.ref_table_fq,
            "template_path": self.template_dir / f"{code}.sql",
        }

    def _check_ext_table(self, table_fq: str) -> None:
        try:
            table = self.client.get_table(table_fq)
        except NotFound:
            raise LookupError(f"外部表不存在: {table_fq}") from None
        if table.table_type != "EXTERNAL":
            raise ValueError(f"{table_fq} 的类型是 {table.table_type}，不是 EXTERNAL")

    def _render(self, template_path: Path, names: dict) -> str:
        if not template_path.is_file():
            raise FileNotFoundError(f"模板不存在: {template_path}")
        text = template_path.read_text(encoding="utf-8")

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

    @staticmethod
    def _extract_body(sql: str, target_view: str) -> str:
        """确认模板以 CREATE OR REPLACE VIEW `target` AS 开头，并返回 SELECT 部分。"""
        m = _HEADER_RE.match(sql)
        if not m:
            raise ValueError("模板必须以 CREATE OR REPLACE VIEW `...` AS 开头")
        if m.group(1) != f"`{target_view}`":
            raise ValueError(f"模板中的目标 view {m.group(1)} 与预期 `{target_view}` 不一致")
        return m.group(2).strip().rstrip(";")

    def _dry_run(self, query: str) -> list:
        cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = self.client.query(query, job_config=cfg, location=self.location)
        return job.schema or []

    @staticmethod
    def _check_schema(schema: list) -> None:
        actual = [(f.name, _TYPE_ALIASES.get(f.field_type, f.field_type)) for f in schema]
        if actual == EXPECTED_SCHEMA:
            return
        problems = []
        for i in range(max(len(actual), len(EXPECTED_SCHEMA))):
            exp = EXPECTED_SCHEMA[i] if i < len(EXPECTED_SCHEMA) else None
            act = actual[i] if i < len(actual) else None
            if exp != act:
                problems.append(f"第{i + 1}列 预期 {exp}，实际 {act}")
        raise ValueError("输出结构不符: " + "; ".join(problems))

    # =======================================================================
    # 日志
    # =======================================================================
    def _ensure_log_table(self) -> None:
        if self._log_ready:
            return
        table = bigquery.Table(self.log_table_fq, schema=LOG_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="started_at"
        )
        self.client.create_table(table, exists_ok=True)
        self._log_ready = True

    def _write_log(self, rec: dict) -> None:
        self._ensure_log_table()
        sql = f"""
            INSERT INTO `{self.log_table_fq}`
              (run_id, run_mode, afs_source_code, src_table, target_view,
               status, failed_step, error_message, started_at, finished_at,
               executed_by, repo_commit)
            VALUES
              (@run_id, @run_mode, @afs_source_code, @src_table, @target_view,
               @status, @failed_step, @error_message, @started_at, @finished_at,
               SESSION_USER(), @repo_commit)
        """
        string_keys = [
            "run_id", "run_mode", "afs_source_code", "src_table", "target_view",
            "status", "failed_step", "error_message",
        ]
        params = [bigquery.ScalarQueryParameter(k, "STRING", rec[k]) for k in string_keys]
        params += [
            bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", rec["started_at"]),
            bigquery.ScalarQueryParameter("finished_at", "TIMESTAMP", rec["finished_at"]),
            bigquery.ScalarQueryParameter("repo_commit", "STRING", self.repo_commit),
        ]
        cfg = bigquery.QueryJobConfig(query_parameters=params)
        self.client.query(sql, job_config=cfg, location=self.location).result()

    # =======================================================================
    # 其他
    # =======================================================================
    def _get_repo_commit(self) -> Optional[str]:
        try:
            out = subprocess.run(
                ["git", "-C", str(self.repo_dir), "rev-parse", "HEAD"],
                capture_output=True, text=True, check=True,
            )
            return out.stdout.strip() or None
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _print_summary(results: list[dict], missing: list[str]) -> None:
        ok = [r for r in results if r["status"] == "success"]
        failed = [r for r in results if r["status"] != "success"]
        print(f"\n===== 汇总：成功 {len(ok)}，失败 {len(failed)} =====")
        for r in failed:
            print(f"  - {r['afs_source_code']} [{r['failed_step']}] {r['error_message']}")
        if missing:
            print("\n有外部表但缺少模板：")
            for code in missing:
                print(f"  - {code}")

"""
source_pipeline.py

Source 阶段 ETL 的核心执行器。

负责：整表拉取并缓存 BigQuery 配置表 -> 按 afs_source_code 校验/取出一行配置 ->
检查 GCS 上是否已有相同版本 -> 下载原始文件 -> 交给 processor 转换成 parquet ->
上传 GCS -> 建 BigQuery 外部表 -> 写运行日志 -> 成功时回写配置表。

设计要点（与 processor 体系解耦）：
- 不 import source_processors.py 里的任何类。processor 类通过构造时传入的
  processor_registry（一个 dict-like 对象，key 是 afs_source_code）按需查找，
  找不到时的默认值由 registry 自己负责（推荐用 collections.defaultdict），
  这样本文件完全不需要知道 BaseSourceProcessor 的存在。
- 一个实例只服务"当前"一个数据源：afs_source_code 通过 set_afs_source_code()
  统一设置和切换，其余所有方法都读取 self.afs_source_code，不再单独接收 code
  参数，避免多处传参互相打架。
- 配置表整表只在构造时（以及显式调用 refresh_cfg() 时）拉取一次并缓存，
  批量运行多个数据源时不会重复查询。
- 配置本身的问题（afs_source_code 未设置 / 配置表里找不到 / download_url
  缺失）一律直接抛出，不在 run() 内部兜底、也不写日志，交给外层调用方处理。
  只有"配置没问题、流程执行中途出错"（下载/转换/上传失败等）才会被 run()
  捕获、记为 FAILED 并写入日志。
"""

import os
import time
import shutil
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
import requests
from tqdm.notebook import tqdm
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound


class SourcePipeline:
    """source 阶段的执行器：下载 -> processor 转换 -> 上传 GCS -> 建外部表 -> 记日志 -> 回写配置表。

    用法：
        pipeline = SourcePipeline(bq_client, gcs_client, bucket_name, dataset_id,
                                   cfg_tbl_name, log_tbl_name,
                                   processor_registry=AFS_SOURCE_PROCESSORS)

        # 一键运行
        pipeline.set_afs_source_code("faostat_rlis")
        result = pipeline.run(force=False, cleanup=False)

        # 分步调试（同一套状态，切换 code 后各步骤都读取当前状态）
        pipeline.set_afs_source_code("faostat_rlis")
        pipeline.download()
        processor = pipeline.build_processor()
        lazy_df = processor.read_and_transform(processor.resolve_input_path())
        ...
    """

    def __init__(
        self,
        bq_client: bigquery.Client,
        gcs_client: storage.Client,
        bucket_name: str,
        dataset_id: str,
        cfg_tbl_name: str,
        log_tbl_name: str,
        processor_registry: Dict[str, Any],
        afs_source_code: Optional[str] = None,
    ):
        """
        cfg_tbl_name / log_tbl_name 用完整的 `project.dataset.table` 格式，
        避免依赖 bq_client 默认的 project/dataset。
        processor_registry 是一个 code -> processor 类 的映射（dict 或
        collections.defaultdict），本类只按需查找，不假设任何具体的默认类。
        afs_source_code 可选：不传的话之后用 set_afs_source_code() 再设置。
        """
        self.bq_client = bq_client
        self.gcs_client = gcs_client
        self.bucket_name = bucket_name
        self.dataset_id = dataset_id
        self.cfg_tbl_name = cfg_tbl_name
        self.log_tbl_name = log_tbl_name
        self.processor_registry = processor_registry

        self.key_column = "afs_source_code"

        self.cfg_df: Optional[pd.DataFrame] = None
        self.refresh_cfg()

        # 当前数据源相关的派生状态，占位；实际值在 set_afs_source_code() 里计算
        self.afs_source_code: Optional[str] = None
        self.local_folder: Optional[str] = None
        self.local_file_path: Optional[str] = None
        self.gcs_blob_name: Optional[str] = None
        self.bq_exttbl_name: Optional[str] = None

        if afs_source_code is not None:
            self.set_afs_source_code(afs_source_code)

    # ------------------------------------------------------------------
    # 配置表：整表拉取 + 缓存
    # ------------------------------------------------------------------
    def refresh_cfg(self) -> pd.DataFrame:
        """从 BigQuery 拉取/刷新整张配置表，缓存为以 afs_source_code 为索引的 DataFrame。
        批量运行多个数据源时只需要调用一次（构造时已自动调用），不必每个 code 都重新拉取。"""
        query = f"SELECT * FROM `{self.cfg_tbl_name}`"
        df = self.bq_client.query(query).to_dataframe()

        if self.key_column not in df.columns:
            raise KeyError(f"配置表中未找到 '{self.key_column}' 列！")

        df = df.set_index(self.key_column)
        df = df[df.index.notnull() & (df.index != "")]
        self.cfg_df = df
        return self.cfg_df

    # ------------------------------------------------------------------
    # 当前数据源：唯一的状态设置入口
    # ------------------------------------------------------------------
    def set_afs_source_code(self, afs_source_code: str) -> None:
        """切换当前处理的数据源，并立即重新计算所有依赖 code 的派生路径/名称
        （local_folder / local_file_path / gcs_blob_name / bq_exttbl_name）。
        这是唯一应该用来改变 pipeline 当前数据源的方法，其余方法都只读取
        self.afs_source_code，不再单独接受 code 参数。

        配置校验（afs_source_code 是否在配置表里、download_url 是否存在）在这里
        就会执行并直接抛出，不做任何兜底——这样一旦切换成功，后续步骤就可以放心
        使用派生出来的路径，而不用担心中途才发现配置有问题。
        """
        self.afs_source_code = afs_source_code
        self.local_folder = f"tmp/{afs_source_code}"
        self.local_file_path = os.path.join(self.local_folder, f"{afs_source_code}_raw")
        self.bq_exttbl_name = f"src_{afs_source_code}_exttbl"

        row = self._get_config_row()  # 配置有问题这里直接抛出
        agency = row.get("agency") or "faostat"
        version = self._resolve_version(row)
        # GCS 路径按 "agency/afs_source_code-version.parquet" 两段式组织：
        # agency 做顶层文件夹分组，afs_source_code 精确到具体数据源。
        self.gcs_blob_name = f"{agency}/{afs_source_code}-{version}.parquet"

    def _require_afs_source_code(self) -> str:
        if not self.afs_source_code:
            raise RuntimeError("请先调用 set_afs_source_code() 指定要处理的数据源")
        return self.afs_source_code

    # ------------------------------------------------------------------
    # 配置行读取与校验
    # ------------------------------------------------------------------
    def _get_config_row(self) -> pd.Series:
        """取出当前 afs_source_code 对应的配置行，并校验必填字段。
        校验失败（code 不在配置表 / download_url 缺失）直接抛出，不做任何兜底。"""
        code = self._require_afs_source_code()

        if code not in self.cfg_df.index:
            raise KeyError(f"配置表中未找到 afs_source_code = '{code}'")

        row = self.cfg_df.loc[code]
        download_url = row.get("download_url")
        if not download_url:
            raise ValueError(f"配置 '{code}' 缺少必填字段 'download_url'")

        return row

    @staticmethod
    def _resolve_version(row: pd.Series) -> str:
        """优先用源头发布日期做版本号；解析失败退化为当前时间戳，
        避免连续运行时用同一个固定字符串互相覆盖 GCS 上的历史版本。"""
        dt = pd.to_datetime(row.get("afs_source_updated_at"), errors="coerce")
        if pd.notnull(dt):
            return dt.strftime("%Y%m%d")
        return datetime.now().strftime("%Y%m%d%H%M%S")

    def _build_afs_src_lst(self, row: pd.Series, afs_source_ingested_at: str) -> Dict[str, str]:
        """打包成需要随数据一起附加到每一行末尾的溯源信息
        （不含 agency —— agency 只用于 GCS 路径分组，不需要跟着数据走）。"""
        return {
            "afs_source_code": self.afs_source_code,
            "afs_source_updated_at": str(row.get("afs_source_updated_at") or ""),
            "afs_source_ingested_at": afs_source_ingested_at,
        }

    # ------------------------------------------------------------------
    # GCS
    # ------------------------------------------------------------------
    def get_gcs_uri(self) -> str:
        self._require_afs_source_code()
        return f"gs://{self.bucket_name}/{self.gcs_blob_name}"

    def check_gcs_version_exists(self) -> bool:
        """独立、无副作用的检查方法：GCS 上是否已存在当前版本的 parquet。
        只负责检查，不做任何"要不要跳过"的决策——决策交给调用方（比如 run()）。"""
        self._require_afs_source_code()
        blob = self.gcs_client.bucket(self.bucket_name).blob(self.gcs_blob_name)
        return blob.exists()

    def upload_to_gcs(self, local_file_path: str) -> None:
        self._require_afs_source_code()
        bucket = self.gcs_client.bucket(self.bucket_name)
        blob = bucket.blob(self.gcs_blob_name)
        blob.upload_from_filename(local_file_path)
        print(f"☁️ 成功上传至 GCS: {self.get_gcs_uri()}")

    # ------------------------------------------------------------------
    # 下载
    # ------------------------------------------------------------------
    def download(self, max_retries: int = 3) -> str:
        """下载原始文件到本地，带指数退避重试。本地已有非空文件会自动跳过——
        这一层缓存判断独立于 run() 的 force 参数，force 只影响 GCS 版本检查那一层，
        想强制重新下载需要自己先删掉本地文件。"""
        code = self._require_afs_source_code()
        row = self._get_config_row()
        download_url = row["download_url"]

        os.makedirs(self.local_folder, exist_ok=True)

        if os.path.exists(self.local_file_path) and os.path.getsize(self.local_file_path) > 0:
            print(f"📦 本地已存在该文件，跳过下载: {self.local_file_path}")
            return self.local_file_path

        last_err: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                print(f"⬇️ 开始下载 (第 {attempt}/{max_retries} 次): {download_url}")
                resp = requests.get(download_url, stream=True, timeout=30)
                resp.raise_for_status()

                total_size = int(resp.headers.get("content-length", 0))
                with (
                    open(self.local_file_path, "wb") as f,
                    tqdm(total=total_size, unit="B", unit_scale=True, desc=code) as pbar,
                ):
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
                return self.local_file_path

            except (requests.RequestException, OSError) as e:
                last_err = e
                print(f"⚠️ 下载失败 (第 {attempt} 次): {e}")
                if os.path.exists(self.local_file_path):
                    os.remove(self.local_file_path)
                if attempt < max_retries:
                    time.sleep(2 ** attempt)  # 指数退避: 2s, 4s, 8s...

        raise RuntimeError(f"下载 '{code}' 失败，已重试 {max_retries} 次: {last_err}")

    # ------------------------------------------------------------------
    # Processor（不 import 具体类，完全依赖外部传入的 processor_registry）
    # ------------------------------------------------------------------
    def build_processor(self, afs_source_ingested_at: Optional[str] = None):
        """用当前数据源实例化对应的 processor。

        processor 类从 processor_registry[afs_source_code] 查找；未注册的 code
        如何处理完全由 registry 自身决定（推荐 registry 用 defaultdict 提供默认类），
        本方法不假设、也不 import 任何具体的 processor 类。

        afs_source_ingested_at 不传时用占位符 "PREVIEW"，方便单步调试时先构造出
        processor、预览 schema，而不用先真正跑一次摄入时间戳。
        """
        code = self._require_afs_source_code()
        row = self._get_config_row()

        ingested_at = afs_source_ingested_at or "PREVIEW"
        afs_src_lst = self._build_afs_src_lst(row, ingested_at)

        processor_cls = self.processor_registry[code]
        return processor_cls(
            raw_file_path=self.local_file_path,
            local_folder=self.local_folder,
            afs_source_code=code,
            afs_src_lst=afs_src_lst,
        )

    # ------------------------------------------------------------------
    # BigQuery 外部表
    # ------------------------------------------------------------------
    def build_external_parquet_table(self) -> bigquery.Table:
        """因为 parquet 自带列信息，这里不需要额外配置 schema。
        会删除同名的旧外部表（如果存在）并重新创建。"""
        self._require_afs_source_code()
        source_uri = self.get_gcs_uri()
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [source_uri]

        table_ref = self.bq_client.dataset(self.dataset_id).table(self.bq_exttbl_name)
        table = bigquery.Table(table_ref)
        table.external_data_configuration = external_config

        self.bq_client.delete_table(table_ref, not_found_ok=True)
        created_table = self.bq_client.create_table(table)
        print(f"✅ BQ Parquet 外部表创建成功: {created_table.full_table_id}")
        return created_table

    # ------------------------------------------------------------------
    # 日志 / 配置回写
    # ------------------------------------------------------------------
    def _ensure_log_table_exists(self, row: Dict[str, Any]) -> None:
        """日志表不存在时，按 row 的字段自动建一张全 STRING 类型的表；已存在就什么都不做。"""
        try:
            self.bq_client.get_table(self.log_tbl_name)
        except NotFound:
            schema = [bigquery.SchemaField(col, "STRING") for col in row]
            table = bigquery.Table(self.log_tbl_name, schema=schema)
            self.bq_client.create_table(table)
            print(f"🆕 [Log] 日志表不存在，已自动创建: '{self.log_tbl_name}'")

    def _log_run(self, row: Dict[str, Any]) -> None:
        """追加一行到 BigQuery 日志表。只记录 pipeline 的运行过程
        （run() 内部调用），不涉及配置本身是否合法这类校验错误。"""
        self._ensure_log_table_exists(row)
        errors = self.bq_client.insert_rows_json(self.log_tbl_name, [row])
        if errors:
            print(f"❌ 写入日志表失败: {errors}")
        else:
            print(f"📝 [Log] 成功记录日志到 '{self.log_tbl_name}'")

    def _update_config_fields(self, fields: Dict[str, Any]) -> None:
        """任务成功后，按 afs_source_code 把若干字段回写到配置表。"""
        if not fields:
            return

        code = self._require_afs_source_code()
        set_clause = ", ".join(f"{col} = @{col}" for col in fields)
        query_parameters = [
            bigquery.ScalarQueryParameter(col, "STRING", str(val)) for col, val in fields.items()
        ]
        query_parameters.append(bigquery.ScalarQueryParameter("key_value", "STRING", code))

        query = f"""
            UPDATE `{self.cfg_tbl_name}`
            SET {set_clause}
            WHERE {self.key_column} = @key_value
        """
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        self.bq_client.query(query, job_config=job_config).result()
        print(f"🔄 [Cfg] 已更新 '{code}': {fields}")
        # 注：如果一次要跑很多个数据源，逐行 UPDATE 可能会触及 BigQuery 的
        # DML 并发/配额限制。数据源数量增长到几十上百时，建议改成先攒好一批结果，
        # 最后用一条 MERGE 语句批量回写，而不是每个 code 单独 UPDATE 一次。

    # ------------------------------------------------------------------
    # 清理
    # ------------------------------------------------------------------
    def cleanup(self) -> None:
        if self.local_folder and os.path.exists(self.local_folder):
            shutil.rmtree(self.local_folder)
            print(f"🧹 临时本地文件已清理: {self.local_folder}")

    # ------------------------------------------------------------------
    # 一键运行
    # ------------------------------------------------------------------
    def run(self, force: bool = False, cleanup: bool = False) -> Dict[str, Any]:
        """跑完整个流水线，返回一个结果 dict（同时也是写入日志表的那一行）。

        - 配置问题（afs_source_code 未设置 / 配置表里找不到 / download_url 缺失）
          不在这里兜底，会直接抛出，交给调用方处理，也不会写入日志表。
        - force=True 时跳过 GCS 版本检查这一层，强制重新走一遍下载/转换/上传；
          本地文件缓存的判断不受 force 影响。
        - 流程执行中的问题（下载/转换/上传/建表失败）会被捕获，记为 FAILED，
          并照常写入日志表。
        - cleanup=True 时运行结束后清理本地临时文件（不论成功/跳过/失败）。
        """
        code = self._require_afs_source_code()
        row = self._get_config_row()  # 配置问题在这里直接抛出，不进入下面的 try

        status = "FAILED"
        message = ""
        gcs_uri = None
        bq_table_id = None
        afs_source_ingested_at = ""

        try:
            if not force and self.check_gcs_version_exists():
                status = "SKIPPED"
                message = f"GCS 中已存在 {self.gcs_blob_name}"
            else:
                self.download()

                afs_source_ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                processor = self.build_processor(afs_source_ingested_at)
                print(f"⚡ 执行数据转换逻辑 (processor: {processor.__class__.__name__})...")
                parquet_path = processor.process()

                self.upload_to_gcs(parquet_path)
                table = self.build_external_parquet_table()

                status = "SUCCESS"
                gcs_uri = self.get_gcs_uri()
                bq_table_id = table.full_table_id

        except Exception as e:
            status = "FAILED"
            message = str(e)

        result = {
            "execution_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "afs_source_code": code,
            "afs_source_updated_at": str(row.get("afs_source_updated_at") or ""),
            "afs_source_ingested_at": afs_source_ingested_at,
            "gcs_uri": gcs_uri or "",
            "bq_table_id": bq_table_id or "",
            "status": status,
            "message": message,
        }

        self._log_run(result)
        if status == "SUCCESS":
            self._update_config_fields({"afs_source_ingested_at": afs_source_ingested_at})

        if cleanup:
            self.cleanup()

        icon = {"SUCCESS": "✅", "SKIPPED": "⏭️", "FAILED": "❌"}.get(status, "❓")
        print(f"{icon} {code}: {status} {message}")

        return result

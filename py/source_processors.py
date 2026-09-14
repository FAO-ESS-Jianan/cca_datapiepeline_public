"""
source_processors.py

各数据源"原始文件 -> 规整 parquet"的转换逻辑。

BaseSourceProcessor 收拢了标准 faostat 结构的默认实现，以及固定不变的收尾流程
(清洗列名 -> 转 Utf8 -> 追加溯源列 -> 落盘 parquet)。结构特殊的数据源只需要
继承并覆盖 read_and_transform()，其余步骤直接复用。

AFS_SOURCE_PROCESSORS 是一个 afs_source_code -> processor 类 的映射，交给
source_pipeline.SourcePipeline 使用。用 defaultdict 提供"未注册的数据源默认
使用 BaseSourceProcessor"的行为，这样 source_pipeline.py 完全不需要 import
或者知道 BaseSourceProcessor 的存在——默认值逻辑留在这一侧。
"""

import os
import zipfile
from collections import defaultdict
from typing import Dict, List, Optional, Type

import polars as pl
from slugify import slugify


class BaseSourceProcessor:
    """所有 processor 的基类。默认 read_and_transform() 按标准 faostat 结构读取，
    特殊数据源继承这个类并只覆盖 read_and_transform() 即可，其余步骤直接复用。"""

    def __init__(self, raw_file_path: str, local_folder: str, afs_source_code: str, afs_src_lst: Dict[str, str]):
        self.raw_file_path = raw_file_path
        self.local_folder = local_folder
        self.afs_source_code = afs_source_code
        self.afs_src_lst = afs_src_lst
        self.input_path: Optional[str] = None  # resolve_input_path() 之后才有值

    def resolve_input_path(self) -> str:
        """如果下载的文件是 zip，解压后返回解压目录；否则直接返回原始文件路径。
        两种情况的返回值最终都会传给 read_and_transform()。"""
        if not zipfile.is_zipfile(self.raw_file_path):
            print(f"📄 非 ZIP 文件，直接使用原始文件: {self.raw_file_path}")
            self.input_path = self.raw_file_path
            return self.input_path

        dest_folder = os.path.join(self.local_folder, "unzipped")
        if os.path.exists(dest_folder) and os.listdir(dest_folder):
            print(f"📂 本地解压目录已存在，跳过解压: {dest_folder}")
            self.input_path = dest_folder
            return self.input_path

        os.makedirs(dest_folder, exist_ok=True)
        with zipfile.ZipFile(self.raw_file_path, "r") as zip_ref:
            zip_ref.extractall(dest_folder)

        self.input_path = dest_folder
        return self.input_path

    @staticmethod
    def _find_normalized_csv(input_path: str) -> str:
        """在目录里找标准 faostat 命名的 csv；如果 input_path 本身就是文件，直接返回。"""
        if os.path.isdir(input_path):
            csv_files = [
                f for f in os.listdir(input_path) if f.endswith("E_All_Data_(Normalized).csv")
            ]
            if not csv_files:
                raise FileNotFoundError(f"在 {input_path} 中未找到标准 CSV (*E_All_Data_(Normalized).csv)")
            return os.path.join(input_path, csv_files[0])
        return input_path

    def read_and_transform(self, input_path: str) -> pl.LazyFrame:
        """默认实现：按 faostat 标准结构读取。特殊数据源在子类里覆盖这个方法。"""
        target_path = self._find_normalized_csv(input_path)
        return pl.scan_csv(target_path, infer_schema=False)

    @staticmethod
    def clean_column_names(cols: List[str]) -> List[str]:
        seen: Dict[str, int] = {}
        cleaned = []
        for i, col in enumerate(cols):
            clean_name = str(col).replace("\x00", "").strip() if col else ""
            slugged = slugify(clean_name, separator="_") or f"unnamed_{i}"
            if slugged in seen:
                seen[slugged] += 1
                final_name = f"{slugged}_{seen[slugged]}"
            else:
                seen[slugged] = 0
                final_name = slugged
            cleaned.append(final_name)
        return cleaned

    def finalize_to_parquet(self, lazy_df: pl.LazyFrame) -> str:
        """固定收尾逻辑，不建议子类覆盖：清洗列名 -> 全部转 Utf8 -> 追加溯源列 -> 落盘 parquet"""
        if lazy_df is None:
            raise RuntimeError(f"read_and_transform 未返回有效的 LazyFrame: {self.afs_source_code}")

        output_parquet_path = os.path.join(self.local_folder, f"{self.afs_source_code}.parquet")

        orig_cols = lazy_df.collect_schema().names()
        rename_dict = dict(zip(orig_cols, self.clean_column_names(orig_cols)))
        lazy_df = lazy_df.rename(rename_dict).select(pl.all().cast(pl.Utf8))

        # 统一附加溯源列（如果 read_and_transform 已经加过同名列，这里会覆盖，保证最终口径一致）
        for col_name, col_value in self.afs_src_lst.items():
            lazy_df = lazy_df.with_columns(pl.lit(col_value).alias(col_name))

        lazy_df.sink_parquet(output_parquet_path)
        print(f"✅ 已成功转换为 Parquet: {output_parquet_path}")
        return output_parquet_path

    def process(self) -> str:
        """模板方法：解压判断 -> 读取转换 -> 收尾落 parquet，返回最终 parquet 路径"""
        input_path = self.resolve_input_path()
        lazy_df = self.read_and_transform(input_path)
        return self.finalize_to_parquet(lazy_df)


# ----------------------------------------------------------------------
# 特殊数据源的 Processor
#
# 大部分数据源直接用 BaseSourceProcessor 的默认实现就够了。少数结构特殊的
# 数据源写一个子类，只覆盖 read_and_transform()：
# ----------------------------------------------------------------------
class WbWdiProcessor(BaseSourceProcessor):
    """需要额外 join 一张 flags 表，并过滤空值。"""

    def read_and_transform(self, input_path: str) -> pl.LazyFrame:
        # 注意：这里读的是 FoodSecurityData_...，和类名 wb_wdi 不太一致，
        # 建议确认是否为笔误 —— 保留原逻辑，未改动。
        main_file = os.path.join(input_path, "FoodSecurityData_E_All_Data_(Normalized).csv")
        flags_file = os.path.join(input_path, "FAOSTAT_Flags.csv")

        lf_main = pl.scan_csv(main_file, ignore_errors=True)
        lf_flags = pl.scan_csv(flags_file, ignore_errors=True)

        return lf_main.join(lf_flags, on="Flag", how="left").filter(
            pl.col("Value").is_not_null()
        )


class FaostatQclProcessor(BaseSourceProcessor):
    """目录下有多个 csv，需要纵向拼接。

    注：目前会把解压目录下所有 .csv 都拼在一起，实际运行中若目录内还包含
    AreaCodes / ItemCodes 等辅助表，会因为 schema 不一致而报错（历史遗留问题，
    这次重构未改动，后续排查时留意。）
    """

    def read_and_transform(self, input_path: str) -> pl.LazyFrame:
        csv_paths = [
            os.path.join(input_path, f)
            for f in os.listdir(input_path)
            if f.endswith(".csv")
        ]
        lazy_frames = [pl.scan_csv(p, ignore_errors=True) for p in csv_paths]
        return pl.concat(lazy_frames, how="vertical")


# ----------------------------------------------------------------------
# 注册表：afs_source_code -> processor 类（不是实例）
#
# 用 defaultdict 提供"未注册的 afs_source_code 默认使用 BaseSourceProcessor"
# 的行为，这样 source_pipeline.py 只需要 processor_registry[code] 就能拿到
# 正确的类，完全不需要 import 或判断 BaseSourceProcessor。
#
# 以后有新的特殊结构数据集，写一个子类并在这里追加一行即可。
# ----------------------------------------------------------------------
AFS_SOURCE_PROCESSORS: Dict[str, Type[BaseSourceProcessor]] = defaultdict(
    lambda: BaseSourceProcessor,
    {
        "wb_wdi": WbWdiProcessor,
        "faostat_qcl": FaostatQclProcessor,
        # "faostat_xx": FaostatXxProcessor,
    },
)

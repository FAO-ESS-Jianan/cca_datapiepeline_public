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
    """World Bank WDI 数据源。

    这一版的两个关键点:

    1. 主表 unpivot 成长表后,立刻过滤掉 Value 为空的行。WDI 数据矩阵本身
       很稀疏,这一步能显著减少行数,而行数直接决定了后面四次 join 的成本——
       行数减半,join 的内存开销大致也跟着减半。

    2. WDISeries.csv 要保留哪些列,直接硬编码成 SERIES_COLUMNS_TO_KEEP。
       列名是固定的,没必要在运行时动态判断,写死列表更清晰也方便审查。
       原则是"只排除真正跟主表重复的信息":逐列核对下来,只有
       "Indicator Name" 与主表完全重复,其余(哪怕是长文本字段)主表里
       都没有对应信息,予以保留。WDIcountry-series.csv / WDIseries-time.csv /
       WDIfootnote.csv 三张表本身没有冗余列,原样全部保留。
    """

    ID_VARS = ["Country Name", "Country Code", "Indicator Name", "Indicator Code"]

    # WDISeries.csv 的列名固定,直接写死要保留哪些列,比运行时动态判断更直观。
    # 只有 "Indicator Name" 因为和主表重复被排除,其余全部保留。
    SERIES_COLUMNS_TO_KEEP = [
        "Series Code",  # join key,join 完会被 _left_join_lookup 当冗余 key 丢弃
        "Topic",
        # "Indicator Name",  # 与主表的 Indicator Name 完全重复,不保留
        # "Short definition", # 这个列是空的
        "Long definition",
        "Unit of measure",
        "Periodicity",
        "Base Period",
        "Other notes",
        "Aggregation method",
        "Limitations and exceptions",
        # "Notes from original source", # 这个列是空的
        # "General comments", # 这个列是空的
        "Source",
        "Statistical concept and methodology",
        "Development relevance",
        # "Related source links", # 这个列是空的
        # "Other web links", # 这个列是空的
        # "Related indicators", # 这个列是空的
        # "License Type", # 不需要
    ]

    def read_and_transform(self, input_path: str) -> pl.LazyFrame:
        lf_main = self._read_main(input_path)
        lf_main = self._merge_series(lf_main, input_path)
        lf_main = self._merge_country_series(lf_main, input_path)
        lf_main = self._merge_series_time(lf_main, input_path)
        lf_main = self._merge_footnote(lf_main, input_path)
        return lf_main

    # ---------- 主表:宽表转长表,并去掉空值行 ----------
    def _read_main(self, input_path: str) -> pl.LazyFrame:
        main_path = os.path.join(input_path, "WDICSV.csv")
        lf = pl.scan_csv(main_path, infer_schema=False, low_memory=True)

        all_cols = lf.collect_schema().names()
        year_cols = [c for c in all_cols if c not in self.ID_VARS]

        return (
            lf.unpivot(
                index=self.ID_VARS,
                on=year_cols,
                variable_name="wb_year",
                value_name="Value",
            )
            .filter(pl.col("Value").is_not_null())  # 空值年份直接丢弃,减少行数
            .with_columns((pl.lit("YR") + pl.col("wb_year")).alias("wb_year_code"))
        )

    # ---------- 小表统一读取入口:eager collect,按 key 去重,再转回 lazy ----------
    @staticmethod
    def _load_lookup(
        path: str,
        key_cols: List[str],
        select_cols: Optional[List[str]] = None,
    ) -> pl.LazyFrame:
        """体积小的 lookup csv 一次性读入内存,按 key 去重(防止 lookup 表本身
        有重复 key 导致 join 时行数爆炸),再转回 lazy 参与 join。"""
        lf = pl.scan_csv(path, infer_schema=False)
        if select_cols is not None:
            lf = lf.select(select_cols)

        df = lf.collect()
        n_before = df.height
        df = df.unique(subset=key_cols, keep="first", maintain_order=False)
        n_after = df.height
        if n_after < n_before:
            print(
                f"⚠️ {os.path.basename(path)} 按 {key_cols} 去重: "
                f"{n_before} -> {n_after} 行,说明该表存在重复 key,已保留第一条。"
            )
        return df.lazy()

    # ---------- 通用 join 工具 ----------
    def _left_join_lookup(
        self,
        lf_main: pl.LazyFrame,
        lf_lookup: pl.LazyFrame,
        on: List[tuple],
    ) -> pl.LazyFrame:
        """on: [(主表列名, lookup 表列名), ...]"""
        main_cols = set(lf_main.collect_schema().names())
        lookup_cols = lf_lookup.collect_schema().names()

        left_keys = [l for l, _ in on]
        right_keys = [r for _, r in on]

        keep_cols = [c for c in lookup_cols if c in right_keys or c not in main_cols]
        lf_lookup = lf_lookup.select(keep_cols)

        joined = lf_main.join(lf_lookup, left_on=left_keys, right_on=right_keys, how="left")

        redundant_keys = [r for l, r in on if r != l]
        joined = joined.drop([c for c in redundant_keys if c in joined.collect_schema().names()])
        return joined

    # ---------- 1. WDISeries.csv ----------
    def _merge_series(self, lf_main: pl.LazyFrame, input_path: str) -> pl.LazyFrame:
        path = os.path.join(input_path, "WDISeries.csv")
        lf_series = self._load_lookup(
            path, key_cols=["Series Code"], select_cols=self.SERIES_COLUMNS_TO_KEEP
        )
        return self._left_join_lookup(lf_main, lf_series, on=[("Indicator Code", "Series Code")])

    # ---------- 2. WDIcountry-series.csv ----------
    def _merge_country_series(self, lf_main: pl.LazyFrame, input_path: str) -> pl.LazyFrame:
        path = os.path.join(input_path, "WDIcountry-series.csv")
        lf = self._load_lookup(path, key_cols=["CountryCode", "SeriesCode"])
        lf = lf.rename({"DESCRIPTION": "NOTE_country_series"})
        return self._left_join_lookup(
            lf_main, lf, on=[("Country Code", "CountryCode"), ("Indicator Code", "SeriesCode")]
        )

    # ---------- 3. WDIseries-time.csv ----------
    def _merge_series_time(self, lf_main: pl.LazyFrame, input_path: str) -> pl.LazyFrame:
        path = os.path.join(input_path, "WDIseries-time.csv")
        lf = self._load_lookup(path, key_cols=["SeriesCode", "Year"])
        lf = lf.rename({"DESCRIPTION": "NOTE_series_time"})
        return self._left_join_lookup(
            lf_main, lf, on=[("Indicator Code", "SeriesCode"), ("wb_year_code", "Year")]
        )

    # ---------- 4. WDIfootnote.csv ----------
    def _merge_footnote(self, lf_main: pl.LazyFrame, input_path: str) -> pl.LazyFrame:
        path = os.path.join(input_path, "WDIfootnote.csv")
        lf = self._load_lookup(path, key_cols=["CountryCode", "SeriesCode", "Year"])
        lf = lf.rename({"DESCRIPTION": "NOTE_footnote"})
        return self._left_join_lookup(
            lf_main, lf,
            on=[
                ("Country Code", "CountryCode"),
                ("Indicator Code", "SeriesCode"),
                ("wb_year_code", "Year"),
            ],
        )


class UnsdSdgProcessor(BaseSourceProcessor):
    """解压后目录下只有一个 csv 文件，文件名不遵循 FAOSTAT 的
    *E_All_Data_(Normalized).csv 命名规则，因此需要自己找 csv 再读取。"""

    def read_and_transform(self, input_path: str) -> pl.LazyFrame:
        csv_files = [f for f in os.listdir(input_path) if f.endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError(f"在 {input_path} 中未找到 csv 文件")
        if len(csv_files) > 1:
            raise RuntimeError(
                f"预期只有一个 csv，但在 {input_path} 中找到多个: {csv_files}"
            )

        target_path = os.path.join(input_path, csv_files[0])
        return pl.scan_csv(target_path, infer_schema=False)

class CustomProcessor(BaseSourceProcessor):
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
        "unstats_sdg": UnsdSdgProcessor,
    },
)

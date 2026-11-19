# -*- coding: utf-8 -*-
"""source_pipeline.py

SourcePipeline — the source-stage ETL. For each `afs_source_code` in the
source config table: download the raw file -> let its processor turn it
into Parquet -> upload it to GCS -> (re)build the BigQuery external table
`src_{afs_source_code}_exttbl` on top of it -> log the run -> on success,
write `afs_source_ingested_at` back to the config table.

Environment-free: every project-specific value (config / log table ids,
the dataset the external tables are created in, the GCS location, the
local working folder) is passed in by the caller. Table ids are always
full ids (`project.dataset.table`), dataset ids are `project.dataset`.

Decoupled from the processors: this module never imports
`source_processors.py`. Processor classes are looked up in the
`processor_registry` passed to the constructor (a dict-like keyed by
`afs_source_code`); what an unregistered code falls back to is up to the
registry itself (e.g. a `collections.defaultdict`).

One instance serves one "current" source at a time:
`set_afs_source_code()` is the only way to switch it, and every step
method reads `self.afs_source_code` instead of taking its own code
argument. `run_batch()` loops over many codes with that same mechanism.

The config table is read once at construction (and again only on an
explicit `refresh_cfg()`), so a batch run never re-queries it per source.

Error contract:
- Config problems (code not in the config table, missing `download_url`)
  raise from `set_afs_source_code()` and are never logged to the log
  table. `run_batch()` catches them and reports the code as FAILED.
- Problems while running (download / transform / upload / table build)
  are caught by `run()`, recorded as FAILED and logged.
"""

from __future__ import annotations

import os
import shutil
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import urlparse

import pandas as pd
import requests
from tqdm.notebook import tqdm
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound


# Column of the config table that identifies a source.
KEY_COLUMN = "afs_source_code"

# Columns of the DataFrame returned by run_batch() (and of each run() result).
RESULT_COLUMNS = [
    "execution_time", "afs_source_code", "afs_source_updated_at", "afs_source_ingested_at",
    "gcs_uri", "bq_table_id", "status", "message",
]

# Some sites (e.g. agincentives.org) reject requests' default
# python-requests User-Agent with a 403, so every download sends browser
# headers. The Referer is set per download from the URL's own domain.
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/130.0 Safari/537.36",
    "Accept": "*/*",
}


def _full_id(value: str, n_parts: int, label: str) -> str:
    """Strip backticks/whitespace and check that `value` is a full id:
    `project.dataset` (n_parts=2) or `project.dataset.table` (n_parts=3)."""
    value = (value or "").strip().strip("`")
    if len(value.split(".")) < n_parts:
        shape = "project.dataset" if n_parts == 2 else "project.dataset.table"
        raise ValueError(f"{label} must be a full id ({shape}), got {value!r}")
    return value


def _split_gcs_uri(uri: str) -> Tuple[str, str]:
    """'gs://bucket/some/prefix' -> ('bucket', 'some/prefix/'); the prefix
    is '' when the URI is just the bucket."""
    uri = (uri or "").strip().rstrip("/")
    if not uri.startswith("gs://"):
        raise ValueError(f"GCS URI must start with gs://, got {uri!r}")
    bucket, _, path = uri[len("gs://"):].partition("/")
    if not bucket:
        raise ValueError(f"GCS URI has no bucket: {uri!r}")
    return bucket, f"{path}/" if path else ""


class SourcePipeline:
    """Source-stage executor: download -> processor -> GCS -> external
    table -> log -> config write-back.

    Usage:
        pipeline = SourcePipeline(
            bq_client=bq_client,
            gcs_client=gcs_client,
            dataset_id="my-project.my_dataset",
            cfg_table_id="my-project.my_dataset.cfg_source_tbl",
            log_table_id="my-project.my_dataset.log_source_tbl",
            gcs_base_uri="gs://my-bucket",
            local_dir="/path/to/work_dir",
            processor_registry=AFS_SOURCE_PROCESSORS,
        )

        # Batch: None = every source in the config table
        results = pipeline.run_batch(["faostat_qcl", "wb_wdi"])

        # One source, step by step (all steps share the current source)
        pipeline.set_afs_source_code("faostat_qcl")
        pipeline.download()
        processor = pipeline.build_processor()
        lazy_df = processor.read_and_transform(processor.resolve_input_path())
    """

    def __init__(
        self,
        bq_client: bigquery.Client,
        gcs_client: storage.Client,
        dataset_id: str,
        cfg_table_id: str,
        log_table_id: str,
        gcs_base_uri: str,
        local_dir: str,
        processor_registry: Dict[str, Any],
        default_agency: str = "faostat",
    ):
        """
        dataset_id: full dataset id (`project.dataset`) the
            `src_{afs_source_code}_exttbl` external tables are created in.
        cfg_table_id / log_table_id: full table ids. The log table is
            created on the first run if it doesn't exist.
        gcs_base_uri: `gs://bucket[/prefix]` the Parquet files are uploaded
            under, as `{agency}/{afs_source_code}-{version}.parquet`.
        local_dir: local working folder; each source gets its own
            `{local_dir}/{afs_source_code}/` subfolder.
        processor_registry: `afs_source_code -> processor class` mapping.
        default_agency: GCS folder used when a config row has no `agency`.
        """
        self.bq_client = bq_client
        self.gcs_client = gcs_client
        self.dataset_id = _full_id(dataset_id, 2, "dataset_id")
        self.cfg_table_id = _full_id(cfg_table_id, 3, "cfg_table_id")
        self.log_table_id = _full_id(log_table_id, 3, "log_table_id")
        self.gcs_base_uri = gcs_base_uri.rstrip("/")
        self.bucket_name, self.gcs_prefix = _split_gcs_uri(gcs_base_uri)
        self.local_dir = local_dir
        self.processor_registry = processor_registry
        self.default_agency = default_agency

        self.cfg_df: pd.DataFrame = pd.DataFrame()
        self.refresh_cfg()

        # State of the current source; set by set_afs_source_code().
        self.afs_source_code: Optional[str] = None
        self.local_folder: Optional[str] = None
        self.local_file_path: Optional[str] = None
        self.gcs_blob_name: Optional[str] = None
        self.exttbl_id: Optional[str] = None

    # ------------------------------------------------------------------
    # Config table — read once, cached
    # ------------------------------------------------------------------

    def refresh_cfg(self) -> pd.DataFrame:
        """(Re)read the whole config table and cache it, indexed by
        afs_source_code. Called by __init__; call again only after the
        config table has been re-synced."""
        df = self.bq_client.query(f"SELECT * FROM `{self.cfg_table_id}`").to_dataframe()
        if KEY_COLUMN not in df.columns:
            raise KeyError(f"Config table {self.cfg_table_id} has no '{KEY_COLUMN}' column.")

        df = df.set_index(KEY_COLUMN)
        self.cfg_df = df[df.index.notnull() & (df.index != "")]
        print(f"Config table loaded: {len(self.cfg_df)} source(s) in {self.cfg_table_id}.")
        return self.cfg_df

    def list_source_codes(self) -> List[str]:
        """Every afs_source_code in the cached config table."""
        return list(self.cfg_df.index)

    # ------------------------------------------------------------------
    # Current source — the only way to switch it
    # ------------------------------------------------------------------

    def set_afs_source_code(self, afs_source_code: str) -> None:
        """Switch the current source and derive everything that depends on
        it (local paths, GCS blob, external table id).

        The config row is checked here (code exists, download_url is set)
        and any problem raises right away, so once this returns, every
        later step can rely on the derived values.
        """
        self.afs_source_code = afs_source_code
        self.local_folder = os.path.join(self.local_dir, afs_source_code)
        self.local_file_path = os.path.join(self.local_folder, f"{afs_source_code}_raw")
        self.exttbl_id = f"{self.dataset_id}.src_{afs_source_code}_exttbl"

        row = self._get_config_row()  # raises on config problems
        agency = row.get("agency") or self.default_agency
        version = self._resolve_version(row)
        # `{agency}/{afs_source_code}-{version}.parquet`: agency groups the
        # files by publisher, afs_source_code pins the exact source.
        self.gcs_blob_name = f"{self.gcs_prefix}{agency}/{afs_source_code}-{version}.parquet"

    def _require_afs_source_code(self) -> str:
        if not self.afs_source_code:
            raise RuntimeError("No current source: call set_afs_source_code() first.")
        return self.afs_source_code

    # ------------------------------------------------------------------
    # Config row
    # ------------------------------------------------------------------

    def _get_config_row(self) -> pd.Series:
        """The current source's config row. Raises if the code isn't in the
        config table or has no download_url."""
        code = self._require_afs_source_code()
        if code not in self.cfg_df.index:
            raise KeyError(f"afs_source_code '{code}' not found in {self.cfg_table_id}.")

        row = self.cfg_df.loc[code]
        if not row.get("download_url"):
            raise ValueError(f"afs_source_code '{code}' has no 'download_url'.")
        return row

    @staticmethod
    def _resolve_version(row: pd.Series) -> str:
        """The source's publish date as the version; falls back to the
        current timestamp so two runs never overwrite each other's file."""
        dt = pd.to_datetime(row.get("afs_source_updated_at"), errors="coerce")
        if pd.notnull(dt):
            return dt.strftime("%Y%m%d")
        return datetime.now().strftime("%Y%m%d%H%M%S")

    def _build_afs_src_lst(self, row: pd.Series, afs_source_ingested_at: str) -> Dict[str, str]:
        """Provenance columns appended to every output row (agency is only
        used for the GCS folder, so it doesn't travel with the data)."""
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
        """Side-effect-free check: is the current version already on GCS?
        Only reports; run() decides whether to skip."""
        self._require_afs_source_code()
        return self.gcs_client.bucket(self.bucket_name).blob(self.gcs_blob_name).exists()

    def upload_to_gcs(self, local_file_path: str) -> None:
        self._require_afs_source_code()
        self.gcs_client.bucket(self.bucket_name).blob(self.gcs_blob_name).upload_from_filename(local_file_path)
        print(f"Uploaded to {self.get_gcs_uri()}")

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def download(self, max_retries: int = 3) -> str:
        """Download the raw file, retrying with exponential backoff. A
        non-empty local file is reused as-is (this cache is independent of
        run()'s `force`; delete the file to force a fresh download).

        4xx errors (403 / 404 ...) are a problem with the request itself,
        so they raise at once; only network errors and 5xx are retried.
        """
        code = self._require_afs_source_code()
        download_url = self._get_config_row()["download_url"]

        os.makedirs(self.local_folder, exist_ok=True)
        if os.path.exists(self.local_file_path) and os.path.getsize(self.local_file_path) > 0:
            print(f"Local file already exists, download skipped: {self.local_file_path}")
            return self.local_file_path

        parsed = urlparse(download_url)
        headers = {**DEFAULT_HEADERS, "Referer": f"{parsed.scheme}://{parsed.netloc}/"}

        last_err: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Downloading (attempt {attempt}/{max_retries}): {download_url}")
                resp = requests.get(download_url, headers=headers, stream=True, timeout=30)
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

            except (requests.RequestException, OSError) as exc:
                last_err = exc
                if os.path.exists(self.local_file_path):
                    os.remove(self.local_file_path)

                status_code = (
                    exc.response.status_code
                    if isinstance(exc, requests.HTTPError) and exc.response is not None
                    else None
                )
                if status_code is not None and 400 <= status_code < 500:
                    raise RuntimeError(
                        f"Download of '{code}' failed (HTTP {status_code}); client errors are not retried: {exc}"
                    ) from exc

                print(f"WARNING: download attempt {attempt} failed: {exc}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)  # 2s, 4s, 8s, ...

        raise RuntimeError(f"Download of '{code}' failed after {max_retries} attempt(s): {last_err}")

    # ------------------------------------------------------------------
    # Processor — looked up in processor_registry, never imported
    # ------------------------------------------------------------------

    def build_processor(self, afs_source_ingested_at: Optional[str] = None):
        """Instantiate the current source's processor from
        processor_registry. Without afs_source_ingested_at the placeholder
        "PREVIEW" is used, so a processor can be built and previewed while
        debugging without a real ingestion timestamp."""
        code = self._require_afs_source_code()
        row = self._get_config_row()
        processor_cls = self.processor_registry[code]
        return processor_cls(
            raw_file_path=self.local_file_path,
            local_folder=self.local_folder,
            afs_source_code=code,
            afs_src_lst=self._build_afs_src_lst(row, afs_source_ingested_at or "PREVIEW"),
        )

    # ------------------------------------------------------------------
    # BigQuery external table
    # ------------------------------------------------------------------

    def build_external_parquet_table(self) -> bigquery.Table:
        """Drop and recreate `src_{afs_source_code}_exttbl` on the current
        GCS file. Parquet carries its own schema, so none is configured."""
        self._require_afs_source_code()
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [self.get_gcs_uri()]

        table = bigquery.Table(self.exttbl_id)
        table.external_data_configuration = external_config

        self.bq_client.delete_table(self.exttbl_id, not_found_ok=True)
        created = self.bq_client.create_table(table)
        print(f"External table created: {created.full_table_id}")
        return created

    # ------------------------------------------------------------------
    # Logging / config write-back
    # ------------------------------------------------------------------

    def _ensure_log_table_exists(self, row: Dict[str, Any]) -> None:
        """Create the log table (all STRING columns, from the row's keys)
        if it doesn't exist yet."""
        try:
            self.bq_client.get_table(self.log_table_id)
        except NotFound:
            schema = [bigquery.SchemaField(col, "STRING") for col in row]
            self.bq_client.create_table(bigquery.Table(self.log_table_id, schema=schema))
            print(f"Created log table `{self.log_table_id}`.")

    def _log_run(self, row: Dict[str, Any]) -> None:
        """Append one row to the log table (pipeline runs only; config
        errors are never logged)."""
        self._ensure_log_table_exists(row)
        errors = self.bq_client.insert_rows_json(self.log_table_id, [row])
        if errors:
            print(f"WARNING: failed to write to {self.log_table_id}: {errors}")

    def _update_config_fields(self, fields: Dict[str, Any]) -> None:
        """Write the given fields back to the current source's config row.

        One UPDATE per source: with many hundreds of sources in one batch,
        BigQuery DML quotas may make a single MERGE at the end preferable.
        """
        if not fields:
            return
        code = self._require_afs_source_code()
        set_clause = ", ".join(f"{col} = @{col}" for col in fields)
        query_parameters = [
            bigquery.ScalarQueryParameter(col, "STRING", str(val)) for col, val in fields.items()
        ]
        query_parameters.append(bigquery.ScalarQueryParameter("key_value", "STRING", code))

        sql = f"""
        UPDATE `{self.cfg_table_id}`
        SET {set_clause}
        WHERE {KEY_COLUMN} = @key_value
        """
        self.bq_client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=query_parameters)).result()
        print(f"Config updated for '{code}': {fields}")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup(self) -> None:
        """Delete the current source's local working folder."""
        if self.local_folder and os.path.exists(self.local_folder):
            shutil.rmtree(self.local_folder)
            print(f"Local files removed: {self.local_folder}")

    # ------------------------------------------------------------------
    # Public entry point — one source
    # ------------------------------------------------------------------

    def run(self, force: bool = False, cleanup: bool = False) -> Dict[str, Any]:
        """Run the whole flow for the current source and return the result
        (the same dict that is written to the log table).

        - force=False skips the source (SKIPPED) when its current version
          is already on GCS; force=True re-runs it anyway. The local file
          cache is not affected by force.
        - Errors while running are caught, recorded as FAILED and logged.
          Config problems raise (see the module docstring).
        - cleanup=True removes the local files afterwards, whatever the
          outcome.
        """
        code = self._require_afs_source_code()
        row = self._get_config_row()  # config problems raise here, outside the try

        status, message = "FAILED", ""
        gcs_uri = bq_table_id = afs_source_ingested_at = ""

        try:
            if not force and self.check_gcs_version_exists():
                status = "SKIPPED"
                message = f"{self.get_gcs_uri()} already exists"
            else:
                self.download()
                afs_source_ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                processor = self.build_processor(afs_source_ingested_at)
                print(f"Transforming with {processor.__class__.__name__}...")
                parquet_path = processor.process()

                self.upload_to_gcs(parquet_path)
                table = self.build_external_parquet_table()

                status = "SUCCESS"
                gcs_uri = self.get_gcs_uri()
                bq_table_id = table.full_table_id
        except Exception as exc:  # noqa: BLE001 - any failure here is a FAILED run, logged below
            status, message = "FAILED", str(exc)

        result = {
            "execution_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "afs_source_code": code,
            "afs_source_updated_at": str(row.get("afs_source_updated_at") or ""),
            "afs_source_ingested_at": afs_source_ingested_at,
            "gcs_uri": gcs_uri,
            "bq_table_id": bq_table_id,
            "status": status,
            "message": message,
        }

        self._log_run(result)
        if status == "SUCCESS":
            self._update_config_fields({"afs_source_ingested_at": afs_source_ingested_at})
        if cleanup:
            self.cleanup()

        print(f"{code} -> {status}" + (f": {message}" if message else ""))
        return result

    # ------------------------------------------------------------------
    # Public entry point — many sources
    # ------------------------------------------------------------------

    def _resolve_codes(self, afs_source_codes: Union[None, str, Iterable[str]]) -> List[str]:
        """None -> every source in the config table; a single code or a list
        -> those codes, de-duplicated in order. Unknown codes are kept, so
        they show up as FAILED instead of disappearing silently."""
        if afs_source_codes is None:
            return self.list_source_codes()
        if isinstance(afs_source_codes, str):
            afs_source_codes = [afs_source_codes]
        return list(dict.fromkeys(afs_source_codes))

    def run_batch(
        self,
        afs_source_codes: Union[None, str, Iterable[str]] = None,
        force: bool = False,
        cleanup: bool = True,
    ) -> pd.DataFrame:
        """run() for each source in turn; one source failing doesn't stop
        the others. afs_source_codes=None runs every source in the config
        table. Returns one row per source (see RESULT_COLUMNS).

        A config error (code not in the config table, no download_url) is
        reported as FAILED here but not written to the log table.
        """
        codes = self._resolve_codes(afs_source_codes)
        results: List[Dict[str, Any]] = []

        for n, code in enumerate(codes, start=1):
            print(f"\n{'=' * 60}\n[{n}/{len(codes)}] {code}\n{'=' * 60}")
            try:
                self.set_afs_source_code(code)
            except (KeyError, ValueError) as exc:
                print(f"{code} -> FAILED: config error: {exc}")
                results.append({"afs_source_code": code, "status": "FAILED", "message": f"Config error: {exc}"})
                continue
            try:
                results.append(self.run(force=force, cleanup=cleanup))
            except Exception as exc:  # noqa: BLE001 - e.g. log/config write failed; keep going
                print(f"{code} -> FAILED: {exc}")
                results.append({"afs_source_code": code, "status": "FAILED", "message": str(exc)})

        df = pd.DataFrame(results, columns=RESULT_COLUMNS)
        counts = df["status"].value_counts().to_dict()
        print(f"\nBatch finished: {len(df)} source(s) — "
              + ", ".join(f"{s}: {counts.get(s, 0)}" for s in ("SUCCESS", "SKIPPED", "FAILED")))
        return df

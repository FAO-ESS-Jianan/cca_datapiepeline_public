# cca_datapipeline

Class definitions and reusable code for the CCA data pipeline, designed to run in a **Google Colab** environment. This is the public, code-only half of the project — environment setup, credentials, and the notebooks that actually invoke this code live in a private repo and run directly from Colab.

## Pipeline stages

Data moves through the pipeline in three stages, each with its own BigQuery config table and log table:

```
Source config (Sheet)          Indicator config (Sheet)
        |                              |
        v  ConfigSheetSyncer           v  ConfigSheetSyncer
cfg_source_tbl (BQ)            cfg_indicator_tbl (BQ)
        |                              |
        v  SourcePipeline              v  StagePipeline
src_*_exttbl (BQ, per source)  stg_* tables (BQ)
```

1. **Config sync** ([py/config_sheet_syncer.py](py/config_sheet_syncer.py)) — keeps a human-edited Google Sheet and its BigQuery config table in sync in both directions, with validation and a sync log.
2. **Source ingestion** ([py/source_pipeline.py](py/source_pipeline.py), [py/source_processors.py](py/source_processors.py)) — downloads a raw source file, converts it to Parquet, uploads it to GCS, and exposes it as a BigQuery external table.
3. **Staging** ([py/stage_pipeline.py](py/stage_pipeline.py)) — builds/refreshes `stg_*` tables from the indicator config, tracking each indicator's status through a review lifecycle.

## Modules

### `py/config_sheet_syncer.py` — `ConfigSheetSyncer`

Generic class for syncing a Google Sheet (human-edited config) with a BigQuery config table. The same class backs both the source-config and indicator/stage-config workflows — only the constructor arguments differ (e.g. whether a tmp review sheet is used, which column is the row key).

- `validate()` — checks the Sheet and BQ table are well-formed and reports column/row differences (non-blocking notes).
- `pull(force=False)` — BigQuery → Sheet.
- `push(force=False)` — Sheet → BigQuery external table → `cfg_table` (`CREATE OR REPLACE`).
- `create_external_table()` — (re)points the BQ external table at the Sheet; call whenever Sheet columns change.
- `write_to_tmp_sheet()` / `promote_tmp_to_main(confirm=True)` — optional human-review buffer before overwriting the main sheet (used by the stage/indicator config, not the source config).

### `py/source_pipeline.py` — `SourcePipeline`

Executor for the source ingestion stage: pulls/caches the source config table, then for a given `afs_source_code` — checks GCS for an existing version, downloads the raw file, hands it to a processor, uploads the resulting Parquet to GCS, creates/refreshes a BigQuery external table, writes a run log, and (on success) writes back to the config table.

It never imports `source_processors.py` — processor classes are looked up at runtime through a `processor_registry` dict passed into the constructor, keeping the two modules decoupled.

```python
pipeline = SourcePipeline(
    bq_client, gcs_client, bucket_name, dataset_id,
    cfg_tbl_name, log_tbl_name,
    processor_registry=AFS_SOURCE_PROCESSORS,
)
pipeline.set_afs_source_code("faostat_rlis")
result = pipeline.run(force=False, cleanup=False)
```

Configuration problems (missing `afs_source_code`, missing `download_url`, etc.) raise immediately. Failures during download/transform/upload are caught, logged as `FAILED`, and don't stop a batch run over multiple sources.

### `py/source_processors.py` — processors

`BaseSourceProcessor` implements the standard FAOSTAT-style read → clean → cast-to-string → append lineage columns → write Parquet pipeline. Sources with a different file layout subclass it and override only `read_and_transform()`:

- `WbWdiProcessor` — joins in a flags file and filters null values.
- `FaostatQclProcessor` — concatenates multiple CSVs found in the extracted archive.

`AFS_SOURCE_PROCESSORS` is a `defaultdict` mapping `afs_source_code → processor class`, defaulting to `BaseSourceProcessor` for any source without a dedicated subclass.

### `py/stage_pipeline.py` — `StagePipeline`

BigQuery-only executor for the staging stage. Reads `cfg_indicator_tbl`, groups indicator filter rules by `(stg_view, stg_tbl)`, and executes one BigQuery job per group. Has no knowledge of the Google Sheet side — that's handled entirely by `ConfigSheetSyncer`.

Status lifecycle: `DRAFT → REVIEW` (via this class) `→ ACTIVE → INACTIVE → DELETED` (manual/human steps).

- `update(confirm_apply=False, confirm_status_update=False)` — `DRAFT` rows only. Appends to (or creates) each `stg_tbl`; on success, promotes affected rows `DRAFT → REVIEW`.
- `reconstruct(stg_view=None, confirm_apply=False)` — `ACTIVE`/`REVIEW`/`INACTIVE` rows. Always `CREATE OR REPLACE`, full rebuild; never touches status.

Both methods default to a dry-run preview and only execute once `confirm_apply=True` is passed. Every `(stg_view, stg_tbl)` group is logged independently, so one failing group doesn't block or roll back the others.

## Design notes

- Each class is decoupled from its neighbors (e.g. `SourcePipeline` doesn't import `source_processors`, `StagePipeline` doesn't know about Sheets) so pieces can be tested, reused, or swapped independently.
- Destructive or write-triggering operations (`run()` with `force=True`, `promote_tmp_to_main()`, `update()`/`reconstruct()`) require an explicit confirmation flag — nothing executes by accident from a default call.
- All BigQuery/GCS clients and credentials are constructed and injected by the calling notebook; these modules hold no auth logic of their own.

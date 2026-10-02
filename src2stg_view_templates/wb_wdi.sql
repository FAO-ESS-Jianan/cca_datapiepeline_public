-- =====================================================================
-- afs_source_code : wb_wdi    (iso3c 匹配型)
-- 占位符由 ViewManager 替换，请勿手动填写：
--   target_view   目标 view
--   source_table  外部表
--   ref_m49_table m49 参照表
-- =====================================================================
CREATE OR REPLACE VIEW {{ target_view }} AS

WITH src AS (
  SELECT *
  FROM {{ source_table }}
),

-- 用 country_code（iso3c）匹配参照表
joined AS (
  SELECT
    src.*,
    xw.iso3c    AS xw_iso3c,
    xw.m49_code AS xw_m49
  FROM src
  LEFT JOIN {{ ref_m49_table }} AS xw
    ON src.country_code = xw.iso3c
)

SELECT
  -- ===== AFS Reference =====
  CAST(xw_m49 AS STRING)                                        AS afs_m49_code,
  CAST(xw_iso3c AS STRING)                                      AS afs_iso3c,
  CAST(country_code AS STRING)                                  AS area_ref,
  CAST(country_name AS STRING)                                  AS area_name,
  SAFE_CAST(wb_year AS INT64)                                   AS afs_year,

  -- ===== Value =====
  -- afs_value：数值；value：原始字符串，无法转换的值 afs_value 为 NULL
  SAFE_CAST(value AS FLOAT64)                                   AS afs_value,
  CAST(value AS STRING)                                         AS value,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'country_code', country_code,
    'country_name', country_name
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'wb_year_code', wb_year_code,
    'wb_year', wb_year,
    'periodicity', periodicity,
    'base_period', base_period
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'indicator_code', indicator_code,
    'indicator_name', indicator_name,
    'topic', topic,
    'long_definition', long_definition
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON '{}'                                                     AS DIMS,

  -- ===== Attribute =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'unit_of_measure', unit_of_measure,
    'other_notes', other_notes,
    'aggregation_method', aggregation_method,
    'limitations_and_exceptions', limitations_and_exceptions,
    'source', `source`,
    'statistical_concept_and_methodology', statistical_concept_and_methodology,
    'development_relevance', development_relevance,
    'note_country_series', note_country_series,
    'note_series_time', note_series_time,
    'note_footnote', note_footnote
  ))                                                            AS ATTRS,

  -- ===== AFS LOG & CONFIG =====
  CAST(afs_source_code AS STRING)                               AS afs_source,
  CAST(afs_source_updated_at AS STRING)                         AS afs_source_updated_at,
  CAST(afs_source_ingested_at AS TIMESTAMP)                     AS afs_source_ingested_at,
  CAST(NULL AS STRING)                                          AS afs_source_priority

FROM joined

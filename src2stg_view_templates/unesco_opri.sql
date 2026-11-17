-- =====================================================================
-- afs_source_code : unesco_opri    (iso3c 匹配型，country_iso_alpha_3_code 为 iso3c)
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

-- 用 country_iso_alpha_3_code（iso3c）匹配参照表
joined AS (
  SELECT
    src.*,
    xw.iso3c    AS xw_iso3c,
    xw.m49_code AS xw_m49
  FROM src
  LEFT JOIN {{ ref_m49_table }} AS xw
    ON src.country_iso_alpha_3_code = xw.iso3c
)

SELECT
  -- ===== AFS Reference =====
  CAST(xw_m49 AS STRING)                                        AS afs_m49_code,
  CAST(xw_iso3c AS STRING)                                      AS afs_iso3c,
  CAST(country_iso_alpha_3_code AS STRING)                      AS area_ref,
  CAST(country_name_english AS STRING)                          AS area_name,
  SAFE_CAST(year AS INT64)                                      AS afs_year,

  -- ===== Value =====
  -- afs_value：数值；value：原始字符串，无法转换的值 afs_value 为 NULL
  -- 源数据无单位列，afs_unit 为 NULL；qualifier（如 UIS_EST）作为 flag
  SAFE_CAST(value AS FLOAT64)                                   AS afs_value,
  CAST(value AS STRING)                                         AS value,
  CAST(NULL AS STRING)                                          AS afs_unit,
  CAST(qualifier AS STRING)                                     AS afs_flag,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'country_iso_alpha_3_code', country_iso_alpha_3_code,
    'country_name_english', country_name_english,
    'unesco_regional_group', unesco_regional_group
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'year', year
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'indicator_id', indicator_id,
    'indicator_english', indicator_english
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON '{}'                                                     AS DIMS,

  -- ===== Attribute =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'magnitude', magnitude,
    'qualifier', qualifier,
    'footnotes', footnotes
  ))                                                            AS ATTRS,

  -- ===== AFS LOG & CONFIG =====
  CAST(afs_source_code AS STRING)                               AS afs_source_code,
  CAST(afs_source_updated_at AS STRING)                         AS afs_source_updated_at,
  CAST(afs_source_ingested_at AS TIMESTAMP)                     AS afs_source_ingested_at

FROM joined

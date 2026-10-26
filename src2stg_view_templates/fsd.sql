-- =====================================================================
-- afs_source_code : fsd    (iso3c 匹配型，iso3 为 iso3c)
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

-- 用 iso3（iso3c）匹配参照表
joined AS (
  SELECT
    src.*,
    xw.iso3c    AS xw_iso3c,
    xw.m49_code AS xw_m49
  FROM src
  LEFT JOIN {{ ref_m49_table }} AS xw
    ON src.iso3 = xw.iso3c
)

SELECT
  -- ===== AFS Reference =====
  CAST(xw_m49 AS STRING)                                        AS afs_m49_code,
  CAST(xw_iso3c AS STRING)                                      AS afs_iso3c,
  CAST(iso3 AS STRING)                                          AS area_ref,
  CAST(country AS STRING)                                       AS area_name,
  -- 数据覆盖一个年份区间，afs_year 取 end_year，缺失时回退到 start_year
  COALESCE(SAFE_CAST(end_year AS INT64), SAFE_CAST(start_year AS INT64)) AS afs_year,

  -- ===== Value =====
  -- afs_value：数值；value：原始字符串，无法转换的值 afs_value 为 NULL
  SAFE_CAST(value AS FLOAT64)                                   AS afs_value,
  CAST(value AS STRING)                                         AS value,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'iso3', iso3,
    'country', country,
    'region', region
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'start_year', start_year,
    'end_year', end_year
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'indicator', indicator
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON '{}'                                                     AS DIMS,

  -- ===== Attribute =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'unit', unit,
    'source', `source`
  ))                                                            AS ATTRS,

  -- ===== AFS LOG & CONFIG =====
  CAST(afs_source_code AS STRING)                               AS afs_source_code,
  CAST(afs_source_updated_at AS STRING)                         AS afs_source_updated_at,
  CAST(afs_source_ingested_at AS TIMESTAMP)                     AS afs_source_ingested_at,
  CAST(NULL AS STRING)                                          AS afs_source_priority

FROM joined

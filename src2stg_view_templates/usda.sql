-- =====================================================================
-- afs_source_code : usda    (iso3c 匹配型，iso3 为 iso3c)
-- 占位符由 ViewManager 替换，请勿手动填写：
--   target_view   目标 view
--   source_table  外部表
--   ref_m49_table m49 参照表
-- 注：fao 列是 FAO 国家代码（非 M49），仅保留在 GEO 中，不参与匹配
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
  CAST(country_territory AS STRING)                             AS area_name,
  SAFE_CAST(year AS INT64)                                      AS afs_year,

  -- ===== Value =====
  -- afs_value：数值；value：原始字符串，无法转换的值 afs_value 为 NULL
  -- 源数据无单位列和 flag 列，afs_unit / afs_flag 为 NULL
  SAFE_CAST(value AS FLOAT64)                                   AS afs_value,
  CAST(value AS STRING)                                         AS value,
  CAST(NULL AS STRING)                                          AS afs_unit,
  CAST(NULL AS STRING)                                          AS afs_flag,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'iso3', iso3,
    'fao', fao,
    'country_territory', country_territory,
    'region', region,
    'sub_region', sub_region,
    'inc_i', inc_i,
    'order', `order`
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'year', year
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'variable', variable
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON '{}'                                                     AS DIMS,

  -- ===== Attribute =====
  JSON '{}'                                                     AS ATTRS,

  -- ===== AFS LOG & CONFIG =====
  CAST(afs_source_code AS STRING)                               AS afs_source_code,
  CAST(afs_source_updated_at AS STRING)                         AS afs_source_updated_at,
  CAST(afs_source_ingested_at AS TIMESTAMP)                     AS afs_source_ingested_at

FROM joined

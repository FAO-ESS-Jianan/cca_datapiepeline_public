-- =====================================================================
-- afs_source_code : fao_fishstat_processed    (m49 匹配型，ref_area 补零)
-- 占位符由 ViewManager 替换，请勿手动填写：
--   target_view   目标 view
--   source_table  外部表
--   ref_m49_table m49 参照表
-- 源表 = 主表 + product 属性表（aquatic_product、product_*）
-- =====================================================================
CREATE OR REPLACE VIEW {{ target_view }} AS

WITH src AS (
  SELECT
    * EXCEPT(ref_area),
    REGEXP_REPLACE(CAST(ref_area AS STRING), r"^'", '') AS ref_area
  FROM {{ source_table }}
),

-- m49_join_key：不足 3 位补零，3 位及以上保持原样
prep AS (
  SELECT
    src.*,
    CASE
      WHEN LENGTH(ref_area) < 3 THEN LPAD(ref_area, 3, '0')
      ELSE ref_area
    END                                                         AS m49_join_key
  FROM src
),

joined AS (
  SELECT
    p.*,
    xw.iso3c    AS xw_iso3c,
    xw.m49_code AS xw_m49
  FROM prep AS p
  LEFT JOIN {{ ref_m49_table }} AS xw
    ON p.m49_join_key = xw.m49_code
)

SELECT
  -- ===== AFS Reference =====
  CAST(xw_m49 AS STRING)                                        AS afs_m49_code,
  CAST(xw_iso3c AS STRING)                                      AS afs_iso3c,
  CAST(m49_join_key AS STRING)                                  AS area_ref,
  CAST(area AS STRING)                                          AS area_name,
  SAFE_CAST(time_period AS INT64)                               AS afs_year,

  -- ===== Value =====
  -- afs_value：数值（未乘 unit_mult）；value：原始字符串，无法转换的值 afs_value 为 NULL
  SAFE_CAST(obs_value AS FLOAT64)                               AS afs_value,
  CAST(obs_value AS STRING)                                     AS value,
  CAST(unit_measure AS STRING)                                  AS afs_unit,
  CAST(obs_status AS STRING)                                    AS afs_flag,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'ref_area', m49_join_key,
    'area', area
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'time_period', time_period,
    'freq', freq
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么（产品 + 计量口径）=====
  -- pproduction_element 为源数据原列名（PPRODUCTION_ELEMENT），保持不变
  JSON_STRIP_NULLS(JSON_OBJECT(
    'product', product,
    'aquatic_product', aquatic_product,
    'product_yearbook_sofia', product_yearbook_sofia,
    'product_fao_major_group', product_fao_major_group,
    'product_isscaap_division', product_isscaap_division,
    'product_isscaap_group', product_isscaap_group,
    'product_cpc_class', product_cpc_class,
    'product_faostat_1', product_faostat_1,
    'product_faostat_2', product_faostat_2,
    'pproduction_element', pproduction_element
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON '{}'                                                     AS DIMS,

  -- ===== Attribute =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'obs_status', obs_status,
    'unit_measure', unit_measure,
    'unit_mult', unit_mult
  ))                                                            AS ATTRS,

  -- ===== AFS LOG & CONFIG =====
  CAST(afs_source_code AS STRING)                               AS afs_source_code,
  CAST(afs_source_updated_at AS STRING)                         AS afs_source_updated_at,
  CAST(afs_source_ingested_at AS TIMESTAMP)                     AS afs_source_ingested_at

FROM joined

-- =====================================================================
-- afs_source_code : faostat_tcl    (标准 area 型)
-- 占位符由 ViewManager 替换，请勿手动填写：
--   target_view   目标 view
--   source_table  外部表
--   ref_m49_table m49 参照表
-- =====================================================================
CREATE OR REPLACE VIEW {{ target_view }} AS

WITH src AS (
  SELECT
    * EXCEPT(area_code_m49),
    REGEXP_REPLACE(CAST(area_code_m49 AS STRING), r"^'", '') AS area_code_m49
  FROM {{ source_table }}
),

-- m49_join_key：不足 3 位补零，3 位及以上保持原样
-- v_raw：去掉首尾空格的原始 value，用于解析
prep AS (
  SELECT
    src.*,
    CASE
      WHEN LENGTH(area_code_m49) < 3 THEN LPAD(area_code_m49, 3, '0')
      ELSE area_code_m49
    END                                                         AS m49_join_key,
    TRIM(CAST(value AS STRING))                                 AS v_raw
  FROM src
),

-- 解析带符号的数值，例如 "<2.5"、"> 100"
val AS (
  SELECT
    prep.*,
    REGEXP_EXTRACT(v_raw, r'^([<>])\s*-?\d+(?:\.\d+)?$')        AS v_sign,
    REGEXP_EXTRACT(v_raw, r'^[<>]\s*(-?\d+(?:\.\d+)?)$')        AS v_num
  FROM prep
),

-- v_dec：数字部分的小数位数；步长 = 最小数位 / 1000 = 10^-(v_dec + 3)
val_dec AS (
  SELECT
    val.*,
    LENGTH(IFNULL(REGEXP_EXTRACT(v_num, r'\.(\d+)$'), ''))      AS v_dec
  FROM val
),

joined AS (
  SELECT
    v.*,
    xw.iso3c    AS xw_iso3c,
    xw.m49_code AS xw_m49
  FROM val_dec AS v
  LEFT JOIN {{ ref_m49_table }} AS xw
    ON v.m49_join_key = xw.m49_code
)

SELECT
  -- ===== AFS Reference =====
  CAST(xw_m49 AS STRING)                                        AS afs_m49_code,
  CAST(xw_iso3c AS STRING)                                      AS afs_iso3c,
  CAST(area_code_m49 AS STRING)                                 AS area_ref,
  CAST(area AS STRING)                                          AS area_name,
  SAFE_CAST(year AS INT64)                                      AS afs_year,

  -- ===== Value =====
  -- "<x"：x - 步长；">x"：x + 步长；纯数字直接转换；其他格式为 NULL
  CASE
    WHEN v_sign = '<' THEN ROUND(CAST(v_num AS FLOAT64) - POW(10.0, -(v_dec + 3)), v_dec + 3)
    WHEN v_sign = '>' THEN ROUND(CAST(v_num AS FLOAT64) + POW(10.0, -(v_dec + 3)), v_dec + 3)
    ELSE SAFE_CAST(v_raw AS FLOAT64)
  END                                                           AS afs_value,
  CAST(value AS STRING)                                         AS value,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'area_code', area_code
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'year_code', year_code,
    'year', year
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'item_code', item_code,
    'item', item,
    'item_code_cpc', item_code_cpc,
    'element_code', element_code,
    'element', element
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON '{}'                                                     AS DIMS,

  -- ===== Attribute =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'flag', flag,
    'note', note,
    'unit', unit
  ))                                                            AS ATTRS,

  -- ===== AFS LOG & CONFIG =====
  CAST(afs_source_code AS STRING)                               AS afs_source,
  CAST(afs_source_updated_at AS STRING)                         AS afs_source_updated_at,
  CAST(afs_source_ingested_at AS TIMESTAMP)                     AS afs_source_ingested_at,
  CAST(NULL AS STRING)                                          AS afs_source_priority

FROM joined

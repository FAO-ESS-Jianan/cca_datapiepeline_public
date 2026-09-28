-- =====================================================================
-- afs_source_code : faostat_rlis    (survey 型)
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

-- survey_code 格式如 "004_2020"：前 3 位 = m49，后 4 位 = 调查年份
-- v_raw：去掉首尾空格的原始 value，用于解析
prep AS (
  SELECT
    src.*,
    LEFT(survey_code, 3)                                        AS m49_join_key,
    RIGHT(survey_code, 4)                                       AS survey_year_raw,
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
  CAST(m49_join_key AS STRING)                                  AS area_ref,
  CAST(survey AS STRING)                                        AS area_name,
  SAFE_CAST(survey_year_raw AS INT64)                           AS afs_year,

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
    'survey_code', survey_code
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'survey_year', survey_year_raw
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'indicator_code', indicator_code,
    'indicator', indicator,
    'element_code', element_code,
    'element', element
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'qualifier_code', qualifier_code,
    'qualifier', qualifier,
    'source_code', source_code,
    'source', `source`
  ))                                                            AS DIMS,

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

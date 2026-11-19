-- =====================================================================
-- afs_source_code : agincentives    (iso3c 匹配型，country_code 为 iso3c)
-- 占位符由 ViewManager 替换，请勿手动填写：
--   target_view   目标 view
--   source_table  外部表
--   ref_m49_table m49 参照表
-- 注：
--   1. 源文件中 year / regionfaocode 等整数列带的 ".0" 已在 AgIncentivesProcessor 中去掉
--   2. 源表有三个数值列，UNPIVOT 成长表，每个源行拆成三行，measure 记录来源列：
--        support_usd             -> 支持额（USD）
--        relevantvalueproduction -> 相关产值（USD）
--        nra                     -> 名义支持率（= support_usd / relevantvalueproduction，比率）
--      UNPIVOT 默认 EXCLUDE NULLS：某个数值列为 NULL 时，对应那一行不出现
--   3. 源数据无 flag 列，afs_flag 为 NULL
-- =====================================================================
CREATE OR REPLACE VIEW {{ target_view }} AS

WITH src AS (
  SELECT *
  FROM {{ source_table }}
),

-- 宽表转长表：三个数值列 -> (measure, value)
long AS (
  SELECT *
  FROM src
  UNPIVOT (
    value FOR measure IN (
      support_usd             AS 'support_usd',
      relevantvalueproduction AS 'relevantvalueproduction',
      nra                     AS 'nra'
    )
  )
),

-- 用 country_code（iso3c）匹配参照表
joined AS (
  SELECT
    long.*,
    xw.iso3c    AS xw_iso3c,
    xw.m49_code AS xw_m49
  FROM long
  LEFT JOIN {{ ref_m49_table }} AS xw
    ON long.country_code = xw.iso3c
)

SELECT
  -- ===== AFS Reference =====
  CAST(xw_m49 AS STRING)                                        AS afs_m49_code,
  CAST(xw_iso3c AS STRING)                                      AS afs_iso3c,
  CAST(country_code AS STRING)                                  AS area_ref,
  CAST(listname_en AS STRING)                                   AS area_name,
  SAFE_CAST(year AS INT64)                                      AS afs_year,

  -- ===== Value =====
  -- afs_value：数值；value：原始字符串，无法转换的值 afs_value 为 NULL
  -- afs_unit：两个 USD 列为 'USD'，nra 为比率，afs_unit 为 NULL
  SAFE_CAST(value AS FLOAT64)                                   AS afs_value,
  CAST(value AS STRING)                                         AS value,
  CASE
    WHEN measure IN ('support_usd', 'relevantvalueproduction') THEN 'USD'
  END                                                           AS afs_unit,
  CAST(NULL AS STRING)                                          AS afs_flag,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'country_code', country_code,
    'listname_en', listname_en,
    'regionfaocode', regionfaocode,
    'regionname', regionname,
    'wbcode2015', wbcode2015,
    'wbname2015', wbname2015
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'year', year
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'measure', measure,
    'nra_cat', nra_cat
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'catprod', catprod,
    'agprocode', agprocode,
    'aggregation', aggregation
  ))                                                            AS DIMS,

  -- ===== Attribute =====
  JSON '{}'                                                     AS ATTRS,

  -- ===== AFS LOG & CONFIG =====
  CAST(afs_source_code AS STRING)                               AS afs_source_code,
  CAST(afs_source_updated_at AS STRING)                         AS afs_source_updated_at,
  CAST(afs_source_ingested_at AS TIMESTAMP)                     AS afs_source_ingested_at

FROM joined

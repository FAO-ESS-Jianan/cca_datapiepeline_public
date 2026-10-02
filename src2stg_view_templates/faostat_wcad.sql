-- =====================================================================
-- afs_source_code : faostat_wcad    (census 型)
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
prep AS (
  SELECT
    src.*,
    CASE
      WHEN LENGTH(area_code_m49) < 3 THEN LPAD(area_code_m49, 3, '0')
      ELSE area_code_m49
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
  CAST(area_code_m49 AS STRING)                                 AS area_ref,
  CAST(area AS STRING)                                          AS area_name,
  -- census_year_code 两种格式：
  --   1) 4 位，如 "2000"：直接作为年份
  --   2) 其他长度，如 "199900"：跨年普查，取结束年
  --      前 2 位 = 开始年的世纪；最后 2 位 = 结束年后两位；
  --      倒数第 3-4 位 = 开始年后两位，仅用于判断是否跨世纪（如 99 -> 00 时世纪 +1）
  CASE
    WHEN LENGTH(CAST(census_year_code AS STRING)) = 4 THEN
      SAFE_CAST(census_year_code AS INT64)
    ELSE
      SAFE_CAST(
        CAST(
          CASE
            WHEN SAFE_CAST(SUBSTR(CAST(census_year_code AS STRING), -2, 2) AS INT64)
               < SAFE_CAST(SUBSTR(CAST(census_year_code AS STRING), -4, 2) AS INT64)
            THEN SAFE_CAST(LEFT(CAST(census_year_code AS STRING), 2) AS INT64) + 1
            ELSE SAFE_CAST(LEFT(CAST(census_year_code AS STRING), 2) AS INT64)
          END AS STRING
        ) || SUBSTR(CAST(census_year_code AS STRING), -2, 2)
        AS INT64
      )
  END                                                           AS afs_year,

  -- ===== Value =====
  -- afs_value：数值；value：原始字符串（如 "<2.5"），无法转换的值 afs_value 为 NULL
  SAFE_CAST(value AS FLOAT64)                                   AS afs_value,
  CAST(value AS STRING)                                         AS value,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'area_code', area_code
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'wca_round_code', wca_round_code,
    'wca_round', wca_round,
    'census_year_code', census_year_code,
    'census_year', census_year
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'item_code', item_code,
    'item', item,
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

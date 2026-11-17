-- =====================================================================
-- afs_source_code : fao_fra_change    (m49 匹配型，ref_area 补零)
-- 占位符由 ViewManager 替换，请勿手动填写：
--   target_view   目标 view
--   source_table  外部表
--   ref_m49_table m49 参照表
-- 源表中不带后缀的列是代码，带 _1 后缀的列是名称，在 src 中统一改名：
--   variable       -> variable_code
--   variable_1     -> variable_label
--   unit_measure   -> unit_measure_code
--   unit_measure_1 -> unit_measure_label
--   decimals       -> decimals_code
--   decimals_1     -> decimals_label
-- =====================================================================
CREATE OR REPLACE VIEW {{ target_view }} AS

WITH src AS (
  SELECT
    * EXCEPT(ref_area, variable, variable_1, unit_measure, unit_measure_1,
             decimals, decimals_1),
    REGEXP_REPLACE(CAST(ref_area AS STRING), r"^'", '') AS ref_area,
    variable       AS variable_code,
    variable_1     AS variable_label,
    unit_measure   AS unit_measure_code,
    unit_measure_1 AS unit_measure_label,
    decimals       AS decimals_code,
    decimals_1     AS decimals_label
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
  -- afs_year：取 time_range（如 "1990-2000"）末尾 4 位数字，即区间终点年份
  SAFE_CAST(REGEXP_EXTRACT(time_range, r"(\d{4})\s*$") AS INT64) AS afs_year,

  -- ===== Value =====
  -- afs_value：数值（未乘 unit_mult）；value：原始字符串，无法转换的值 afs_value 为 NULL
  SAFE_CAST(obs_value AS FLOAT64)                               AS afs_value,
  CAST(obs_value AS STRING)                                     AS value,
  CAST(unit_measure_code AS STRING)                             AS afs_unit,
  CAST(obs_status AS STRING)                                    AS afs_flag,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'ref_area', m49_join_key,
    'area', area,
    'ref_area_iso2', ref_area_iso2,
    'ref_area_iso3', ref_area_iso3
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'time_period', time_period,
    'time_range', time_range,
    'freq', freq,
    'frequency', frequency
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'variable_code', variable_code,
    'variable_label', variable_label
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON '{}'                                                     AS DIMS,

  -- ===== Attribute =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'obs_status', obs_status,
    'observation_status', observation_status,
    'unit_measure_code', unit_measure_code,
    'unit_measure_label', unit_measure_label,
    'unit_mult', unit_mult,
    'unit_multiplier', unit_multiplier,
    'decimals_code', decimals_code,
    'decimals_label', decimals_label
  ))                                                            AS ATTRS,

  -- ===== AFS LOG & CONFIG =====
  CAST(afs_source_code AS STRING)                               AS afs_source_code,
  CAST(afs_source_updated_at AS STRING)                         AS afs_source_updated_at,
  CAST(afs_source_ingested_at AS TIMESTAMP)                     AS afs_source_ingested_at

FROM joined

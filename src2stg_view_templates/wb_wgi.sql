-- =====================================================================
-- afs_source_code : wb_wgi    (iso3c 匹配型，ref_area 为 iso3c)
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

-- 用 ref_area（iso3c）匹配参照表
joined AS (
  SELECT
    src.*,
    xw.iso3c    AS xw_iso3c,
    xw.m49_code AS xw_m49
  FROM src
  LEFT JOIN {{ ref_m49_table }} AS xw
    ON src.ref_area = xw.iso3c
)

SELECT
  -- ===== AFS Reference =====
  CAST(xw_m49 AS STRING)                                        AS afs_m49_code,
  CAST(xw_iso3c AS STRING)                                      AS afs_iso3c,
  CAST(ref_area AS STRING)                                      AS area_ref,
  CAST(ref_area_label AS STRING)                                AS area_name,
  SAFE_CAST(time_period AS INT64)                               AS afs_year,

  -- ===== Value =====
  -- afs_value：数值；value：原始字符串，无法转换的值 afs_value 为 NULL
  SAFE_CAST(obs_value AS FLOAT64)                               AS afs_value,
  CAST(obs_value AS STRING)                                     AS value,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'ref_area', ref_area,
    'ref_area_label', ref_area_label
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'time_period', time_period,
    'freq', freq,
    'freq_label', freq_label,
    'time_format', time_format,
    'time_format_label', time_format_label
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'indicator', indicator,
    'indicator_label', indicator_label
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'sex', sex,
    'sex_label', sex_label,
    'age', age,
    'age_label', age_label,
    'urbanisation', urbanisation,
    'urbanisation_label', urbanisation_label,
    'comp_breakdown_1', comp_breakdown_1,
    'comp_breakdown_1_label', comp_breakdown_1_label,
    'comp_breakdown_2', comp_breakdown_2,
    'comp_breakdown_2_label', comp_breakdown_2_label,
    'comp_breakdown_3', comp_breakdown_3,
    'comp_breakdown_3_label', comp_breakdown_3_label
  ))                                                            AS DIMS,

  -- ===== Attribute =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'structure', structure,
    'structure_id', structure_id,
    'action', action,
    'unit_measure', unit_measure,
    'unit_measure_label', unit_measure_label,
    'unit_mult', unit_mult,
    'unit_mult_label', unit_mult_label,
    'unit_type', unit_type,
    'unit_type_label', unit_type_label,
    'decimals', decimals,
    'decimals_label', decimals_label,
    'database_id', database_id,
    'database_id_label', database_id_label,
    'obs_status', obs_status,
    'obs_status_label', obs_status_label,
    'obs_conf', obs_conf,
    'obs_conf_label', obs_conf_label
  ))                                                            AS ATTRS,

  -- ===== AFS LOG & CONFIG =====
  CAST(afs_source_code AS STRING)                               AS afs_source_code,
  CAST(afs_source_updated_at AS STRING)                         AS afs_source_updated_at,
  CAST(afs_source_ingested_at AS TIMESTAMP)                     AS afs_source_ingested_at,
  CAST(NULL AS STRING)                                          AS afs_source_priority

FROM joined

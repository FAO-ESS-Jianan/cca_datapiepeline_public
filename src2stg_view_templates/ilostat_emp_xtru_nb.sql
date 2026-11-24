-- =====================================================================
-- afs_source_code : ilostat_emp_xtru_nb    (m49 匹配型，ref_area_m49code 补零)
-- 占位符由 ViewManager 替换，请勿手动填写：
--   target_view   目标 view
--   source_table  外部表
--   ref_m49_table m49 参照表
-- 注：
--   1. ref_area_m49code 由 IlostatProcessor 根据 ref_area_label 名称生成（3 位 M49）；
--      未匹配上的名称、以及 ILO 自定义聚合区域（名称带冒号，如 "World: Low income"）为 NULL，
--      此时 afs_m49_code / afs_iso3c 也为 NULL
--   2. 源表所有列均为 STRING；obs_value 无法转成数值时 afs_value 为 NULL
--   3. 源数据无单位列，afs_unit 为 NULL（该指标为人数，单位见 indicator_label）
--   4. obs_status_label 作为 afs_flag
--   5. 同一国家-年份可能有多个数据来源（source_label），所以 source_label 放在 DIMS 里
--   6. ilostat_emp_dwap_rt / ilostat_emp_xtru_rt 结构相同，可复制本文件使用
-- =====================================================================
CREATE OR REPLACE VIEW {{ target_view }} AS

WITH src AS (
  SELECT
    * EXCEPT(ref_area_m49code),
    REGEXP_REPLACE(CAST(ref_area_m49code AS STRING), r"^'", '') AS ref_area_m49code
  FROM {{ source_table }}
),

-- m49_join_key：不足 3 位补零，3 位及以上保持原样
prep AS (
  SELECT
    src.*,
    CASE
      WHEN LENGTH(ref_area_m49code) < 3 THEN LPAD(ref_area_m49code, 3, '0')
      ELSE ref_area_m49code
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
  CAST(ref_area_label AS STRING)                                AS area_name,
  -- time 通常为 4 位年份；若出现 2019Q1 / 2019M01 这类写法，取前 4 位
  SAFE_CAST(REGEXP_EXTRACT(`time`, r'^(\d{4})') AS INT64)       AS afs_year,

  -- ===== Value =====
  -- afs_value：数值；value：原始字符串，无法转换的值 afs_value 为 NULL
  SAFE_CAST(obs_value AS FLOAT64)                               AS afs_value,
  CAST(obs_value AS STRING)                                     AS value,
  CAST(NULL AS STRING)                                          AS afs_unit,
  CAST(obs_status_label AS STRING)                              AS afs_flag,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'ref_area_m49code', m49_join_key,
    'ref_area_label', ref_area_label
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'time', `time`
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'indicator_label', indicator_label
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'sex_label', sex_label,
    'classif1_label', classif1_label,
    'classif2_label', classif2_label,
    'source_label', source_label
  ))                                                            AS DIMS,

  -- ===== Attribute =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'obs_status_label', obs_status_label,
    'note_classif_label', note_classif_label,
    'note_indicator_label', note_indicator_label,
    'note_source_label', note_source_label
  ))                                                            AS ATTRS,

  -- ===== AFS LOG & CONFIG =====
  CAST(afs_source_code AS STRING)                               AS afs_source_code,
  CAST(afs_source_updated_at AS STRING)                         AS afs_source_updated_at,
  CAST(afs_source_ingested_at AS TIMESTAMP)                     AS afs_source_ingested_at

FROM joined

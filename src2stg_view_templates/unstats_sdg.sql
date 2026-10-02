-- =====================================================================
-- afs_source_code : unstats_sdg    (m49 匹配型，geoareacode 补零)
-- 占位符由 ViewManager 替换，请勿手动填写：
--   target_view   目标 view
--   source_table  外部表
--   ref_m49_table m49 参照表
-- 源数据用字符串 'NA' 表示缺失，统一用 NULLIF 转为 NULL
-- =====================================================================
CREATE OR REPLACE VIEW {{ target_view }} AS

WITH src AS (
  SELECT
    * EXCEPT(geoareacode),
    CAST(geoareacode AS STRING) AS geoareacode
  FROM {{ source_table }}
),

-- m49_join_key：不足 3 位补零（如 World "1" -> "001"），3 位及以上保持原样
prep AS (
  SELECT
    src.*,
    CASE
      WHEN LENGTH(geoareacode) < 3 THEN LPAD(geoareacode, 3, '0')
      ELSE geoareacode
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
  CAST(NULLIF(geoareaname, 'NA') AS STRING)                     AS area_name,
  SAFE_CAST(timeperiod AS INT64)                                AS afs_year,

  -- ===== Value =====
  -- afs_value：数值；value：原始字符串，无法转换的值（包括 'NA'）afs_value 为 NULL
  SAFE_CAST(value AS FLOAT64)                                   AS afs_value,
  CAST(value AS STRING)                                         AS value,

  -- ===== Geographical Reference =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'geoareacode', m49_join_key,
    'geoareaname', NULLIF(geoareaname, 'NA')
  ))                                                            AS GEO,

  -- ===== Time Scale =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'timeperiod', timeperiod,
    'time_detail', NULLIF(time_detail, 'NA'),
    'timecoverage', NULLIF(timecoverage, 'NA'),
    'freq', NULLIF(freq, 'NA')
  ))                                                            AS TIME_PERIOD,

  -- ===== Series：测的是什么 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'goal', NULLIF(goal, 'NA'),
    'target', NULLIF(`target`, 'NA'),
    'indicator', NULLIF(indicator, 'NA'),
    'seriescode', seriescode,
    'seriesid', seriesid,
    'seriesdescription', NULLIF(seriesdescription, 'NA'),
    'isdsdseries', NULLIF(isdsdseries, 'NA')
  ))                                                            AS SERIES,

  -- ===== Dimensions：按什么拆分 =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'age', NULLIF(age, 'NA'),
    'sex', NULLIF(sex, 'NA'),
    'location', NULLIF(location, 'NA'),
    'level_status', NULLIF(level_status, 'NA'),
    'name_of_international_agreement', NULLIF(name_of_international_agreement, 'NA'),
    'education_level', NULLIF(education_level, 'NA'),
    'type_of_product', NULLIF(type_of_product, 'NA'),
    'type_of_facilities', NULLIF(type_of_facilities, 'NA'),
    'name_of_international_institution', NULLIF(name_of_international_institution, 'NA'),
    'type_of_occupation', NULLIF(type_of_occupation, 'NA'),
    'tariff_regime_status', NULLIF(tariff_regime_status, 'NA'),
    'type_of_skill', NULLIF(type_of_skill, 'NA'),
    'mode_of_transportation', NULLIF(mode_of_transportation, 'NA'),
    'type_of_mobile_technology', NULLIF(type_of_mobile_technology, 'NA'),
    'name_of_non_communicable_disease', NULLIF(name_of_non_communicable_disease, 'NA'),
    'type_of_speed', NULLIF(type_of_speed, 'NA'),
    'migratory_status', NULLIF(migratory_status, 'NA'),
    'disability_status', NULLIF(disability_status, 'NA'),
    'hazard_type', NULLIF(hazard_type, 'NA'),
    'ihr_capacity', NULLIF(ihr_capacity, 'NA'),
    'cities', NULLIF(cities, 'NA'),
    'reporting_type', NULLIF(reporting_type, 'NA'),
    'quantile', NULLIF(quantile, 'NA'),
    'activity', NULLIF(activity, 'NA'),
    'policy_domains', NULLIF(policy_domains, 'NA'),
    'policy_instruments', NULLIF(policy_instruments, 'NA'),
    'sampling_stations', NULLIF(sampling_stations, 'NA'),
    'type_of_waste_treatment', NULLIF(type_of_waste_treatment, 'NA'),
    'grounds_of_discrimination', NULLIF(grounds_of_discrimination, 'NA'),
    'parliamentary_committees', NULLIF(parliamentary_committees, 'NA'),
    'cause_of_death', NULLIF(cause_of_death, 'NA'),
    'substance_use_disorders', NULLIF(substance_use_disorders, 'NA'),
    'mountain_elevation', NULLIF(mountain_elevation, 'NA'),
    'deviation_level', NULLIF(deviation_level, 'NA'),
    'frequency_of_chlorophyll_a_concentration', NULLIF(frequency_of_chlorophyll_a_concentration, 'NA'),
    'food_waste_sector', NULLIF(food_waste_sector, 'NA'),
    'fiscal_intervention_stage', NULLIF(fiscal_intervention_stage, 'NA'),
    'level_of_requirement', NULLIF(level_of_requirement, 'NA'),
    'type_of_support', NULLIF(type_of_support, 'NA'),
    'report_ordinal', NULLIF(report_ordinal, 'NA'),
    'counterpart', NULLIF(counterpart, 'NA'),
    'government_name', NULLIF(government_name, 'NA'),
    'severity_of_price_levels', NULLIF(severity_of_price_levels, 'NA'),
    'level_of_government', NULLIF(level_of_government, 'NA'),
    'type_of_renewable_technology', NULLIF(type_of_renewable_technology, 'NA'),
    'population_group', NULLIF(population_group, 'NA'),
    'custom_breakdown', NULLIF(custom_breakdown, 'NA'),
    'service_attribute', NULLIF(service_attribute, 'NA'),
    'land_cover', NULLIF(land_cover, 'NA'),
    'bioclimatic_belt', NULLIF(bioclimatic_belt, 'NA'),
    'illicit_financial_flows', NULLIF(illicit_financial_flows, 'NA'),
    'nutrient_loading', NULLIF(nutrient_loading, 'NA'),
    'type_of_ofdi_scheme', NULLIF(type_of_ofdi_scheme, 'NA'),
    'marine_spatial_planning_msp', NULLIF(marine_spatial_planning_msp, 'NA'),
    'composite_breakdown', NULLIF(composite_breakdown, 'NA'),
    'type_of_household', NULLIF(type_of_household, 'NA')
  ))                                                            AS DIMS,

  -- ===== Attribute =====
  JSON_STRIP_NULLS(JSON_OBJECT(
    'id', id,
    'units', NULLIF(units, 'NA'),
    'unitmultiplier', NULLIF(unitmultiplier, 'NA'),
    'nature', NULLIF(nature, 'NA'),
    'seriesobservationcount', seriesobservationcount,
    'releasestatus', NULLIF(releasestatus, 'NA'),
    'releasename', NULLIF(releasename, 'NA'),
    'valuetype', NULLIF(valuetype, 'NA'),
    'upperbound', NULLIF(upperbound, 'NA'),
    'lowerbound', NULLIF(lowerbound, 'NA'),
    'baseperiod', NULLIF(baseperiod, 'NA'),
    'source', NULLIF(`source`, 'NA'),
    'geoinfourl', NULLIF(geoinfourl, 'NA'),
    'footnote', NULLIF(footnote, 'NA'),
    'observationid', observationid,
    'observation_status', NULLIF(observation_status, 'NA')
  ))                                                            AS ATTRS,

  -- ===== AFS LOG & CONFIG =====
  CAST(afs_source_code AS STRING)                               AS afs_source,
  CAST(afs_source_updated_at AS STRING)                         AS afs_source_updated_at,
  CAST(afs_source_ingested_at AS TIMESTAMP)                     AS afs_source_ingested_at,
  CAST(NULL AS STRING)                                          AS afs_source_priority

FROM joined

with ghg_base as (

  SELECT
    CAST(facility_id AS INT64) as facility_id,
    facility_name,
    city,
    state,
    CAST(naics_code AS INT64) as naics_code,
    year,
    SPLIT(industry_sector, ',')[SAFE_OFFSET(0)] AS industry_sector1,
    SPLIT(industry_sector, ',')[SAFE_OFFSET(1)] AS industry_sector2,
    SPLIT(industry_sector, ',')[SAFE_OFFSET(2)] AS industry_sector3,
    _data_source,
    _load_time
  FROM {{ source('us_climate_raw', 'facility_ghg_emissions') }}

),

with_llm_org as (

  SELECT
    ghg_base.*,
    LOWER(TRIM(llm.organization_name)) AS raw_org_name
  FROM ghg_base
  LEFT JOIN `kiaraerica.dbt_us_climate_int.tmp_ghg_facilities_llm_org_checkpoint` llm
    ON CAST(ghg_base.facility_id AS STRING) = llm.facility_id

),

with_standardized_org as (

  SELECT
    with_llm_org.*,
    mapping.standardized_name
  FROM with_llm_org
  LEFT JOIN `kiaraerica.dbt_us_climate_int.tmp_ghg_organization_name_mapping` mapping
    ON LOWER(TRIM(with_llm_org.raw_org_name)) = LOWER(TRIM(mapping.original_name))

),

final_facility as (

  SELECT
    with_standardized_org.*,
    org.organization_name AS organization
  FROM with_standardized_org
  LEFT JOIN `kiaraerica.dbt_us_climate_int.Organizations` org
    ON LOWER(TRIM(with_standardized_org.standardized_name)) = LOWER(TRIM(org.organization_name))

)

SELECT
  year,
  facility_id,
  facility_name,
  organization,
  city,
  state,
  naics_code,
  industry_sector1,
  industry_sector2,
  industry_sector3,
  _data_source,
  _load_time
FROM final_facility
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY year, facility_id, facility_name
  ORDER BY _load_time DESC
) = 1


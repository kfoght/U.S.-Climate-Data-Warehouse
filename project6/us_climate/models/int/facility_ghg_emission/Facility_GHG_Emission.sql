with tmp_facility_ghg_emission as (
  select distinct
    GENERATE_UUID() as emission_id,
    year,
    CAST(facility_id AS INT64) as facility_id,
    facility_name,
    carbon_dioxide_emissions,
    methane_emissions,
    nitrous_oxide_emissions,
    biogenic_co2_emissions,
    _data_source,
    _load_time
  from {{ source('us_climate_raw', 'facility_ghg_emissions') }}
)

select *
from tmp_facility_ghg_emission
order by facility_id, year

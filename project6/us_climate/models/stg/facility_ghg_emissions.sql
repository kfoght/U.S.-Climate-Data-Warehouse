with stg_facility_ghg_emissions as (
    SELECT
        facility_id,
        facility_name,
        city,
        state,
        naics_code,
        year,
        SPLIT(industry_sector, ',')[SAFE_OFFSET(0)] AS industry_sector1,
        SPLIT(industry_sector, ',')[SAFE_OFFSET(1)] AS industry_sector2,
        SPLIT(industry_sector, ',')[SAFE_OFFSET(2)] AS industry_sector3,
        max_rated_heat_input_capacity,
        carbon_dioxide_emissions,
        methane_emissions,
        nitrous_oxide_emissions,
        biogenic_co2_emissions,
        _data_source,
        _load_time
    from {{ source('us_climate_raw', 'facility_ghg_emissions') }}
)

select *
from stg_facility_ghg_emissions
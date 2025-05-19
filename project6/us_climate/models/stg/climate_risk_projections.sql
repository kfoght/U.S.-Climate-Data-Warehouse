with stg_climate_risk_projections as(
    select geo_id as county_code,
        temp_change,
        precipitation_change,
        extreme_precipitation,
        extreme_cold,
        extreme_heat,
        dry_change,
        impervious_surface,
        housing_density,
        population_estimate,
        low_lying_houses,
        `low-lying_roads` as low_lying_roads,
        hazard,
        exposure,
        vulnerability,
        risk_percentage,
        _data_source,
        _load_time
    from {{ source('us_climate_raw', 'climate_risk_projections') }}
)

select *
from stg_climate_risk_projections
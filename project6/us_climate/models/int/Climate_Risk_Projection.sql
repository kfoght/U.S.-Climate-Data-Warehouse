with int_tmp_crp as(
    select r.county_code,
        c.state,
        c.county,
        r.temp_change,
        r.precipitation_change,
        r.extreme_precipitation,
        r.extreme_cold,
        r.extreme_heat,
        r.dry_change,
        r.impervious_surface,
        r.housing_density,
        r.population_estimate,
        r.low_lying_houses,
        r.low_lying_roads,
        r.hazard,
        r.exposure,
        r.vulnerability,
        r.risk_percentage,
        r._data_source,
        r._load_time
  from {{ ref('climate_risk_projections') }} r
  left join {{ ref('county_codes') }} c
  on r.county_code = c.county_code
),

int_Climate_Risk_Projection as (
    select a.county_code,
        s.geo_id as state,
        a.county,
        a.temp_change,
        a.precipitation_change,
        a.extreme_precipitation,
        a.extreme_cold,
        a.extreme_heat,
        a.dry_change,
        a.impervious_surface,
        a.housing_density,
        a.population_estimate,
        a.low_lying_houses,
        a.low_lying_roads,
        a.hazard,
        a.exposure,
        a.vulnerability,
        a.risk_percentage,
        a._data_source,
        a._load_time
  from int_tmp_crp a
  left join {{ ref('Geo_References')}} s
  on a.state = s.name
)

select *
from int_Climate_Risk_Projection
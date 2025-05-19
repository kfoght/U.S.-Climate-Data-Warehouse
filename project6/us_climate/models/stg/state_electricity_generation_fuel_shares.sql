with stg_state_electricity_generation_fuel_shares as(
    select
        state,
        nuclear,
        coal,
        natural_gas,
        petroleum,
        hydro,
        geothermal,
        solar_power,
        wind,
        biomass_and_others,
        _data_source,
        _load_time
    from {{ source('us_climate_raw', 'state_electricity_generation_fuel_shares') }}
)

select *
from stg_state_electricity_generation_fuel_shares
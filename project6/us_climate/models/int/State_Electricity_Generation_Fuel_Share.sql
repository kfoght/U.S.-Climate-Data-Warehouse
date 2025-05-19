with int_tmp_state_elec as (
    select
        case
            when state = 'New York2' then 'New York'
            when state = 'Michigan1' then 'Michigan'
        else state
        end as state,
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
from {{ ref('state_electricity_generation_fuel_shares') }}
),

int_State_Electricity_Generation_Fuel_Share as (
    select
        s.geo_id as state,
        e.nuclear,
        e.coal,
        e.natural_gas,
        e.petroleum,
        e.hydro,
        e.geothermal,
        e.solar_power,
        e.wind,
        e.biomass_and_others,
        e._data_source,
        e._load_time
  from int_tmp_state_elec e
  left join {{ ref('Geo_References') }} s
  on e.state = s.name
)

select *
from int_State_Electricity_Generation_Fuel_Share
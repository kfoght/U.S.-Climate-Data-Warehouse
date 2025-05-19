with stg_state_average_temperature as(
    select
        month,
        year,
        state,
        average_temp,
        monthly_mean_from_1901_to_2000,
        _data_source,
        _load_time
    from {{ source('us_climate_raw', 'state_average_temperature') }}
)

select *
from stg_state_average_temperature
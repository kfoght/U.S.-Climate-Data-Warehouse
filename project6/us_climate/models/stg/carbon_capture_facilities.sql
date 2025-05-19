with stg_carbon_capture_facilities as (
    select id,
        facility,
        organization,
        city,
        state,
        category,
        status,
        industry,
        _data_source,
        _load_time
    from {{ source('us_climate_raw', 'carbon_capture_facilities') }}
)

select *
from stg_carbon_capture_facilities
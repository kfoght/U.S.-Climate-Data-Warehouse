with stg_state_climate_policies as (
    select state, policy, policy_area, category, status,
        case year_enacted when '' then null else safe_cast(year_enacted as INTEGER) end as year_enacted,
        _data_source, _load_time
    from {{ source('us_climate_raw', 'state_climate_policies') }}
)

select *
from stg_state_climate_policies
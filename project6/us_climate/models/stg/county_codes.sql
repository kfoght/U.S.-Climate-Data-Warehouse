with stg_county_codes as(
    select state,
        county,
        fips_state,
        fips_county,
        county_code,
        _data_source,
        _load_time
    from {{ source('us_climate_raw', 'county_codes') }}
)

select *
from stg_county_codes
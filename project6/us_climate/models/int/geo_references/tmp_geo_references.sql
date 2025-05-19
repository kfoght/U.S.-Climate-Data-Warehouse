with int_tmp_geo_state_disasters as (
    select distinct state as geo_id
    from {{ ref('state_disasters')  }}
),
int_tmp_geo_state_ghg as (
    select distinct geo_ref as geo_id
    from {{ ref('state_ghg_emissions') }}
    where geo_ref not in (select geo_id from int_tmp_geo_state_disasters)
),
int_tmp_geo_carbon as (
    select distinct state as geo_id
    from {{ ref('carbon_capture_facilities') }}
    where state not in (select geo_id from int_tmp_geo_state_disasters)
)

select
    geo_id,
    null as name,
    null as capital
from int_tmp_geo_state_disasters
union distinct
select geo_id, null, null from int_tmp_geo_state_ghg
union distinct
select geo_id, null, null from int_tmp_geo_carbon
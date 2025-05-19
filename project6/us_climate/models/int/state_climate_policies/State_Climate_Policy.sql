-- state_climate_policies has 'Hawai'i'
-- Geo_References has 'Hawaii'
-- before pre_hook state had null values

{{ config(
    pre_hook = [
        "update {{ ref('state_climate_policies' )}} set state = 'Hawaii' where state like 'Hawa%'"
    ]
) }}

with int_State_Climate_Policy as (
    select
        s.geo_id as state,
        p.policy,
        p.status,
        p.year_enacted,
        p._data_source,
        p._load_time
    from {{ ref('state_climate_policies') }} p
    left join {{ ref('Geo_References') }} s
    on p.state = s.name
)

select *
from int_State_Climate_Policy
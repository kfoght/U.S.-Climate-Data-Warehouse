with int_tmp_policy as (
    select distinct policy, policy_area, category
    from {{ ref ('state_climate_policies')}}
)

select *
from int_tmp_policy
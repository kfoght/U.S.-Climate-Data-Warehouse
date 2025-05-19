with int_Policies as(
    select distinct
        p.policy,
        p.policy_area,
        p.category,
        s._data_source,
        s._load_time
  from {{ ref('tmp_policy_filtered') }} p
  left join {{ ref('state_climate_policies') }} s
  on p.policy = s.policy
)

select *
from int_Policies
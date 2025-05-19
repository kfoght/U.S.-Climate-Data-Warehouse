with mrt_top_policy_categories_by_state as (
    select p.category, g.name as state, count(*) as count
    from {{ ref('Policies') }} p
    join {{ ref('State_Climate_Policy') }} s
    on s.policy = p.policy
    join {{ ref('Geo_References') }} g
    on s.state = g.geo_id
    group by p.category, g.name
    order by count(*) desc, g.name
)

select *
from mrt_top_policy_categories_by_state
order by count desc, state
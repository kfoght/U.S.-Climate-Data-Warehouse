with mrt_lowest_avg_temps_by_state_by_year as (
    select s.name as state, t.year, min(t.average_temp) as min_temp
    from {{ ref('State_Average_Temperature') }} t
    join {{ ref('Geo_References') }} s
    on t.state = s.geo_id
    group by s.name, t.year
    order by s.name, t.year
)

select *
from mrt_lowest_avg_temps_by_state_by_year
order by state, year

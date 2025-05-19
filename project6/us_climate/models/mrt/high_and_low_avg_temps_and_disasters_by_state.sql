with mrt_high_and_low_avg_temps_and_disasters_by_state as (
    select h.state, h.year, l.min_temp, h.max_temp, drought_count, flooding_count,
        freeze_count, severe_storm_count, tropical_cyclone_count,
       wildfire_count, winter_storm_count
    from {{ ref('highest_avg_temps_by_state_by_year') }} h
    join {{ ref('lowest_avg_temps_by_state_by_year') }} l on (h.state = l.state and h.year = l.year)
    join {{ ref('Geo_References') }} g on h.state = g.name
    join {{ ref('State_Disaster') }} s on (g.geo_id = s.state and h.year = s.year)
    order by h.state, h.year
)

select *
from mrt_high_and_low_avg_temps_and_disasters_by_state
order by state, year
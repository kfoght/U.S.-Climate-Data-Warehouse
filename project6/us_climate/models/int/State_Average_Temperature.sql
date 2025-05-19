with int_State_Average_Temperature as(
    select t.month,
        t.year,
        s.geo_id as state,
        t.average_temp,
        t.monthly_mean_from_1901_to_2000,
        t._data_source,
        t._load_time
  from {{ ref('state_average_temperature')}} t
  left join {{ ref('Geo_References') }} s
  on t.state = s.name
)

select *
from int_State_Average_Temperature
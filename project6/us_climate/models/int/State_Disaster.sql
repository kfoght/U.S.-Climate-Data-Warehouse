with int_State_Disaster as (
    select state,
        year,
        drought_count,
        drought_cost_min,
        drought_cost_max,
        flooding_count,
        flooding_cost_min,
        flooding_cost_max,
        freeze_count,
        freeze_cost_min,
        freeze_cost_max,
        severe_storm_count,
        severe_storm_cost_min,
        severe_storm_cost_max,
        tropical_cyclone_count,
        tropical_cyclone_cost_min,
        tropical_cyclone_cost_max,
        wildfire_count,
        wildfire_cost_min,
        wildfire_cost_max,
        winter_storm_count,
        winter_storm_cost_min,
        winter_storm_cost_max,
        all_disasters_count,
        all_disasters_cost_min,
        all_disasters_cost_max,
        _data_source,
        _load_time
  from {{ ref('state_disasters') }}
)

select *
from int_State_Disaster
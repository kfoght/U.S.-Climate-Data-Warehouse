with stg_state_disasters as(
    SELECT
        state,
        year,
        drought_count,
        SAFE_CAST(SPLIT(drought_cost_range, '-')[SAFE_OFFSET(0)] AS INT64) AS drought_cost_min,
        SAFE_CAST(SPLIT(drought_cost_range, '-')[SAFE_OFFSET(1)] AS INT64) AS drought_cost_max,
        flooding_count,
        SAFE_CAST(SPLIT(flooding_cost_range, '-')[SAFE_OFFSET(0)] AS INT64) AS flooding_cost_min,
        SAFE_CAST(SPLIT(flooding_cost_range, '-')[SAFE_OFFSET(1)] AS INT64) AS flooding_cost_max,
        freeze_count,
        SAFE_CAST(SPLIT(freeze_cost_range, '-')[SAFE_OFFSET(0)] AS INT64) AS freeze_cost_min,
        SAFE_CAST(SPLIT(freeze_cost_range, '-')[SAFE_OFFSET(1)] AS INT64) AS freeze_cost_max,
        severe_storm_count,
        SAFE_CAST(SPLIT(severe_storm_cost_range, '-')[SAFE_OFFSET(0)] AS INT64) AS severe_storm_cost_min,
        SAFE_CAST(SPLIT(severe_storm_cost_range, '-')[SAFE_OFFSET(1)] AS INT64) AS severe_storm_cost_max,
        tropical_cyclone_count,
        SAFE_CAST(SPLIT(tropical_cyclone_cost_range, '-')[SAFE_OFFSET(0)] AS INT64) AS tropical_cyclone_cost_min,
        SAFE_CAST(SPLIT(tropical_cyclone_cost_range, '-')[SAFE_OFFSET(1)] AS INT64) AS tropical_cyclone_cost_max,
        wildfire_count,
        SAFE_CAST(SPLIT(wildfire_cost_range, '-')[SAFE_OFFSET(0)] AS INT64) AS wildfire_cost_min,
        SAFE_CAST(SPLIT(wildfire_cost_range, '-')[SAFE_OFFSET(1)] AS INT64) AS wildfire_cost_max,
        winter_storm_count,
        SAFE_CAST(SPLIT(winter_strom_cost_range, '-')[SAFE_OFFSET(0)] AS INT64) AS winter_storm_cost_min,
        SAFE_CAST(SPLIT(winter_strom_cost_range, '-')[SAFE_OFFSET(1)] AS INT64) AS winter_storm_cost_max,
        all_disasters_count,
        SAFE_CAST(SPLIT(all_disasters_cost_range, '-')[SAFE_OFFSET(0)] AS INT64) AS all_disasters_cost_min,
        SAFE_CAST(SPLIT(all_disasters_cost_range, '-')[SAFE_OFFSET(1)] AS INT64) AS all_disasters_cost_max,
        _data_source,
        _load_time
    from {{ source('us_climate_raw', 'state_disasters') }}
)

select *
from stg_state_disasters
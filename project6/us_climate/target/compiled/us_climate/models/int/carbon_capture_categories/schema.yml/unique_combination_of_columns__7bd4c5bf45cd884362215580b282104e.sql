





with validation_errors as (

    select
        facility_id, category
    from `kiaraerica`.`dbt_us_climate_int`.`Carbon_Capture_Categories`
    group by facility_id, category
    having count(*) > 1

)

select *
from validation_errors



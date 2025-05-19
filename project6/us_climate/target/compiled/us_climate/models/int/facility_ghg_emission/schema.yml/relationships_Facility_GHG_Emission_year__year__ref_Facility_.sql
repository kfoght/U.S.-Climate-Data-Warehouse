
    
    

with child as (
    select year as from_field
    from `kiaraerica`.`dbt_us_climate_int`.`Facility_GHG_Emission`
    where year is not null
),

parent as (
    select year as to_field
    from `kiaraerica`.`dbt_us_climate_int`.`Facility`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



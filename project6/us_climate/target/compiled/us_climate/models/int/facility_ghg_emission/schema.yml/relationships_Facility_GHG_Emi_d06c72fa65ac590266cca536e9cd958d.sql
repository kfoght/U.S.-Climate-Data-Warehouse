
    
    

with child as (
    select facility_name as from_field
    from `kiaraerica`.`dbt_us_climate_int`.`Facility_GHG_Emission`
    where facility_name is not null
),

parent as (
    select facility_name as to_field
    from `kiaraerica`.`dbt_us_climate_int`.`Facility`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



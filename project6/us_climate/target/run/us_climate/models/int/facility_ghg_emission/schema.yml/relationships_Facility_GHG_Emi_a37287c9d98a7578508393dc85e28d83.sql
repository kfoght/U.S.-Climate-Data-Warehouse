select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select facility_id as from_field
    from `kiaraerica`.`dbt_us_climate_int`.`Facility_GHG_Emission`
    where facility_id is not null
),

parent as (
    select facility_id as to_field
    from `kiaraerica`.`dbt_us_climate_int`.`Facility`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test
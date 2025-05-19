
    
    

with dbt_test__target as (

  select emission_id as unique_field
  from `kiaraerica`.`dbt_us_climate_int`.`Facility_GHG_Emission`
  where emission_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



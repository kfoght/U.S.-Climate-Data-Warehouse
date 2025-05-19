
    
    

with dbt_test__target as (

  select organization_name as unique_field
  from `kiaraerica`.`dbt_us_climate_int`.`Organizations`
  where organization_name is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



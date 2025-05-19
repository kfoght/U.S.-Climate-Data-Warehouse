select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select organization_name
from `kiaraerica`.`dbt_us_climate_int`.`Organizations`
where organization_name is null



      
    ) dbt_internal_test
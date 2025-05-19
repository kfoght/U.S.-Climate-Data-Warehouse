select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select category
from `kiaraerica`.`dbt_us_climate_int`.`Carbon_Capture_Categories`
where category is null



      
    ) dbt_internal_test
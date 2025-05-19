select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select facility_id
from `kiaraerica`.`dbt_us_climate_int`.`Carbon_Capture_Categories`
where facility_id is null



      
    ) dbt_internal_test
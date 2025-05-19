select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select emission_id
from `kiaraerica`.`dbt_us_climate_int`.`Facility_GHG_Emission`
where emission_id is null



      
    ) dbt_internal_test
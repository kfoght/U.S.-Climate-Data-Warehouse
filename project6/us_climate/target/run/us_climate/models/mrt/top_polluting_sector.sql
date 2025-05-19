
  
    

    create or replace table `kiaraerica`.`dbt_us_climate_mrt`.`top_polluting_sector`
      
    
    

    OPTIONS()
    as (
      with mrt_top_polluting_sector as (
    select
        sector,
        sum(case when ghg = 'Carbon Dioxide' then emission else null end) as sector_co2_emissions,
        sum(case when ghg = 'Methane' then emission else null end) as sector_methane_emissions,
        sum(case when ghg = 'Nitrous Oxide' then emission else null end) as sector_n2o_emissions
    from `kiaraerica`.`dbt_us_climate_int`.`State_GHG_Emission`
    group by sector
    order by sector_co2_emissions desc
)

select *
from mrt_top_polluting_sector
order by sector_co2_emissions desc
    );
  
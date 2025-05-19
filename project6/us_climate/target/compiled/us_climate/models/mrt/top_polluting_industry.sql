with emissions_by_industry as (

    select
        f.industry_sector1 as industry_sector,
        sum(e.carbon_dioxide_emissions) as industry_co2_emissions,
        sum(e.methane_emissions) as industry_methane_emissions,
        sum(e.nitrous_oxide_emissions) as industry_n2o_emissions
    from `kiaraerica`.`dbt_us_climate_int`.`Facility_GHG_Emission` e
    join `kiaraerica`.`dbt_us_climate_int`.`Facility` f
      on e.facility_id = f.facility_id
     and e.facility_name = f.facility_name
     and e.year = f.year
    group by f.industry_sector1

)

select *
from emissions_by_industry
order by industry_co2_emissions desc
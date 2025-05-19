with mrt_top_polluting_state as (
    select
        geo_ref as state,
        sum(case when ghg = 'Carbon Dioxide' then emission else null end) as state_co2_emissions,
        sum(case when ghg = 'Methane' then emission else null end) as state_methane_emissions,
        sum(case when ghg = 'Nitrous Oxide' then emission else null end) as state_n2o_emissions
    from `kiaraerica`.`dbt_us_climate_int`.`State_GHG_Emission`
    group by state
    order by state_co2_emissions desc
)

select *
from mrt_top_polluting_state
    order by state_co2_emissions desc
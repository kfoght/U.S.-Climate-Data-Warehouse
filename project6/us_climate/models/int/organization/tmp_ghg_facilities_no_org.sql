with tmp_ghg_facilities_no_org as (
  select *
  from (
    select
      facility_id,
      facility_name,
      city,
      state,
      naics_code,
      industry_sector1,
      industry_sector2,
      industry_sector3,
      max_rated_heat_input_capacity,
      carbon_dioxide_emissions,
      methane_emissions,
      nitrous_oxide_emissions,
      biogenic_co2_emissions,
      row_number() over (partition by facility_id order by facility_id desc) as rk
    from us_climate_stg.facility_ghg_emissions
    where facility_name is not null
  )
  where rk = 1
  order by facility_name
)

select *
from tmp_ghg_facilities_no_org
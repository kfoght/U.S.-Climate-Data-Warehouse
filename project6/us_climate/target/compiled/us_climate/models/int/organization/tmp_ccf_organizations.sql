with combined_orgs as (

  select
    organization as organization_name
  from `kiaraerica`.`us_climate_raw`.`carbon_capture_facilities`
  where lower(organization) not in ('', 'n/a', 'na', 'none', 'null', 'unknown')

  union all

  select
    organization_name as organization_name
  from `kiaraerica`.`dbt_us_climate_int`.`tmp_ccf_facilities_llm_org`
  where lower(organization_name) not in ('', 'n/a', 'na', 'none', 'null', 'unknown')

)

select distinct organization_name
from combined_orgs
order by organization_name
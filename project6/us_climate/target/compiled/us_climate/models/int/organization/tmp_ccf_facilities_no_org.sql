with tmp_ccf_facilities_no_org as (
  select distinct
    id as ccf_id,
    facility as ccf_facility_name,
    organization,
    city,
    state,
    category,
    status,
    industry
  from `kiaraerica`.`us_climate_raw`.`carbon_capture_facilities`
  where organization is null
  order by facility
)

select *
from tmp_ccf_facilities_no_org
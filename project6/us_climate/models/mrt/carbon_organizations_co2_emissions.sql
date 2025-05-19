with joined as (

  select
    c.organization,
    e.carbon_dioxide_emissions
  from {{ ref('Carbon_Capture_Facility') }} c
  join {{ ref('Facility') }} f 
    on c.organization = f.organization
  join {{ ref('Facility_GHG_Emission') }} e 
    on f.year = e.year 
    and f.facility_id = e.facility_id 
    and f.facility_name = e.facility_name

),

co2_totals as (

  select
    organization,
    sum(carbon_dioxide_emissions) as total_co2_emissions
  from joined
  group by organization

)

select *
from co2_totals
order by total_co2_emissions desc

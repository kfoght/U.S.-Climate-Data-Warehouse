with joined as (

  select 
    f.organization,
    c.category
  from `kiaraerica`.`dbt_us_climate_int`.`Carbon_Capture_Facility` f
  join `kiaraerica`.`dbt_us_climate_int`.`Carbon_Capture_Categories` c 
    on f.id = c.facility_id

),

category_counts as (

  select
    organization,
    category,
    count(*) as count
  from joined
  group by organization, category

)

select *
from category_counts
order by count desc, organization, category
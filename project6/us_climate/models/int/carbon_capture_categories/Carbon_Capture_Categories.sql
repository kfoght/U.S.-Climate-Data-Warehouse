with base as (

  select
    id as facility_id,
    split(categories, ',') as category_array,
    _data_source,
    _load_time
  from {{ ref('Carbon_Capture_Facility') }}

),

exploded as (

  select
    facility_id,
    trim(category) as category,
    _data_source,
    _load_time
  from base, unnest(category_array) as category

)

select *
from exploded
order by facility_id, category

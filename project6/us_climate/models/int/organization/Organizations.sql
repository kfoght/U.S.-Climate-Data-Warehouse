with combined_orgs as (

  select
    standardized_name as organization_name,
    country
  from {{ ref('tmp_ccf_organization_name_mapping') }}
  where lower(standardized_name) not in ('', 'n/a', 'na', 'none', 'null', 'unknown')

  union all

  select
    standardized_name as organization_name,
    country
  from {{ ref('tmp_ghg_organization_name_mapping') }}
  where lower(standardized_name) not in ('', 'n/a', 'na', 'none', 'null', 'unknown')

),

deduped as (

  select
    organization_name,
    max(country) as country
  from combined_orgs
  group by organization_name

)

select *
from deduped
order by organization_name

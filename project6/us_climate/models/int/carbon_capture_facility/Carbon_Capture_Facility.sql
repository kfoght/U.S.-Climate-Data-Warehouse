with base_ccf as (
  select
    id,
    facility,
    organization,
    city,
    state,
    category as categories,
    status,
    industry,
    _data_source,
    _load_time
  from {{ source('us_climate_raw', 'carbon_capture_facilities') }}
),

with_llm_org as (
  select
    base_ccf.*,
    lower(trim(llm.organization_name)) as raw_org_name
  from base_ccf
  left join {{ ref('tmp_ccf_facilities_llm_org') }} as llm
    on cast(base_ccf.id as string) = cast(llm.ccf_id as string)
),

with_standardized_org as (
  select
    with_llm_org.*,
    mapping.standardized_name
  from with_llm_org
  inner join {{ ref('tmp_ccf_organization_name_mapping') }} as mapping
    on lower(trim(coalesce(with_llm_org.raw_org_name, with_llm_org.organization))) = lower(trim(cast(mapping.original_name as string)))
),

with_final_name as (
  select
    *,
    coalesce(standardized_name, organization) as final_org_name
  from with_standardized_org
),

filtered as (
  select
    f.*
  from with_final_name f
  inner join {{ ref('Organizations') }} o
    on lower(trim(f.final_org_name)) = lower(trim(o.organization_name))
),

final_ccf as (
  select
    id,
    facility,
    final_org_name as organization,
    city,
    state,
    categories,
    status,
    industry,
    _data_source,
    _load_time
  from filtered
)

select *
from final_ccf
qualify row_number() over (
  partition by facility, organization, city, state, categories, status, industry
  order by id
) = 1

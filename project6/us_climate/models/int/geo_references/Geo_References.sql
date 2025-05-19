with int_Geo_References as (
    select distinct
        geo_ref.geo_id,
        geo_ref.name,
        geo_ref.capital,
        coalesce(sd._data_source, ghg._data_source, cc._data_source) as _data_source,
        coalesce(sd._load_time, ghg._load_time, cc._load_time) as _load_time
    from {{ ref('tmp_geo_references_enriched') }} geo_ref
    left join {{ ref('state_disasters') }} sd
    on geo_ref.geo_id = sd.state
    left join {{ ref('state_ghg_emissions') }} ghg
    on geo_ref.geo_id = ghg.geo_ref
    left join {{ ref('carbon_capture_facilities') }} cc
    on geo_ref.geo_id = cc.state
)

select *
from int_Geo_References
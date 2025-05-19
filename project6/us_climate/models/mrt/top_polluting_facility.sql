with emissions_by_facility as (

    select
        f.facility_id,
        f.facility_name,
        f.state,
        sum(e.carbon_dioxide_emissions) as facility_co2_emissions,
        sum(e.methane_emissions) as facility_methane_emissions,
        sum(e.nitrous_oxide_emissions) as facility_n2o_emissions
    from {{ ref('Facility_GHG_Emission') }} e
    join {{ ref('Facility') }} f
      on e.facility_id = f.facility_id
     and e.facility_name = f.facility_name
     and e.year = f.year
    group by f.facility_id, f.facility_name, f.state

)

select *
from emissions_by_facility
order by facility_co2_emissions desc
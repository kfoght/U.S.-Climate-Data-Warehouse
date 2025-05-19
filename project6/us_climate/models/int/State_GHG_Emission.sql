-- In the original State_GHG_Emission table the various fields set to be primary keys had nulls so tests failed
-- To fix this :
-- 1. Unpivot table so the multiple years are not each a separate field
-- 2. Summed emission values when all field values were the same
-- 3. Created emission_id to use as PK that is a string of all the separate fields that you have been PK's

with int_tmp_state_ghg_unpivot as (
    select
        geo_ref,
        cast(substring(year, 2) as int64) as year,
        ghg,
        fuel,
        sector,
        subsector,
        econ_sector,
        econ_subsector,
        category,
        sub_category_1,
        sub_category_2,
        sub_category_3,
        emission,
        _data_source,
        _load_time
    from {{ ref('state_ghg_emissions') }}
    unpivot(
        emission for year in (
            Y1990, Y1991, Y1992, Y1993, Y1994, Y1995,
            Y1996, Y1997, Y1998, Y1999, Y2000, Y2001,
            Y2002, Y2003, Y2004, Y2005, Y2006, Y2007,
            Y2008, Y2009, Y2010, Y2011, Y2012, Y2013,
            Y2014, Y2015, Y2016, Y2017, Y2018, Y2019,
            Y2020, Y2021, Y2022
        )
    )
),

int_tmp_state_ghg_aggregated as (
    select geo_ref, year, ghg, fuel, sector, subsector, econ_sector, econ_subsector,
        category, sub_category_1, sub_category_2, sub_category_3, sum(emission) as emission,
        _data_source, _load_time
    from int_tmp_state_ghg_unpivot
    group by geo_ref, year, ghg, fuel, sector, subsector, econ_sector, econ_subsector,
        category, sub_category_1, sub_category_2, sub_category_3, _data_source, _load_time
),

int_State_GHG_Emission as (
    select
        array_to_string([
            geo_ref,
            cast(year as string),
            ghg,
            fuel,
            sector,
            subsector,
            econ_sector,
            econ_subsector,
            category,
            sub_category_1,
            sub_category_2,
            sub_category_3
        ], ', ') as emission_id,
        geo_ref,
        year,
        ghg,
        fuel,
        sector,
        subsector,
        econ_sector,
        econ_subsector,
        category,
        sub_category_1,
        sub_category_2,
        sub_category_3,
        emission,
        _data_source,
        _load_time,
from int_tmp_state_ghg_aggregated
)

select *
from int_State_GHG_Emission
# Project 2 Criteria Analysis
### Criteria 1
We have 7 different data sources: NASA, US Enviornmental Protection Agency (EPA), Climate XChange, National Oceanic and Atmospheric Association (NOAA), Nuclear Energy Institute (NEI), etc.

### Criteria 2
The `state_climate_policies` and `carbon_capture_facilities` tables were both created from pdf.

### Criteria 3
The logical entities include: State Average Temperature, State Disasters, Climate Risk Projections, Facility GHG Emissions, State GHG Emissions, State Climate Policies, Carbon Capture Facilities, State Electricity Generation Fuel Shares.

### Criteria 4
All data in the warehouse is consistent and functional dependencies hold within a table and across records. The facility_GHG_emissions table ensures consistency by enforcing that each facility_id is uniquely associated with a specific facility_name, city, and state.

### Criteria 5
`state_climate_policies.year_enacted` stores a numeric value as a string.

### Criteria 6
`state_climate_policies.year_enacted` stores an empty string for null values.

### Criteria 7
The fields `drought_cost_range`, `flooding_cost_range`, etc., in the `state_disasters` table contains the min and max as a range of possible values. The field `facility_ghg_emissions.industry_sector` contains various industry types in the same field. They can be separated as this is a means to join with other tables in the future.

### Criteria 8
`facility_ghg_emissions` sometimes stores a list of industry types and sectors in the same cell. `carbon_capture_facilities` sometimes stores a list of categories in the same cell.

### Criteria 9
Geographic location information is repeated across tables in different ways. `climate_risk_projections.geo_id`, which uses county codes, versus `state_disasters.state` and `state_average_temperature.state`. Industry categories are also represented in different ways, `carbon_capture_facilities.industry` versus `state_ghg_emissions.sector`.

### Criteria 10
The `state_ghg_emissions` table has information about emissions categorized in economic sectors and UNFCC reporting sectors. The fields `econ_sector`, `econ_subsector`, `sector`, and `subsector` in the same table are redundant as they often have the same values.
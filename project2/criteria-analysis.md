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
`carbon_capture_facilities` stores a list of categories in the same cell.

### Criteria 9
Geographic location information is repeated across tables in different ways. `climate_risk_projections.geo_id`, which uses county codes, versus `state_disasters.state` and `state_average_temperature.state`. `carbon_capture_facilities` table has an organization column while `facility_ghg_emissions` does not, however they both have a facility column.

### Criteria 10
In the `state_climate_policies` table, `policy`, `policy_area`, and `category` are grouped uniquely. The unique combinations can be store in a separate table to remove redundancy.

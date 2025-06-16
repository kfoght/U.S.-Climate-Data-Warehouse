# U.S.-Climate-Data-Warehouse
Collaborated with another student on building a cloud-based data warehouse on Google Cloud Platform (GCP).

## Project 1
Picked a domain, U.S. climate, and found datasets relating to that domain. Found 6 datasets that contained emission, temperature, policy, disaster and risk data. After collecting the datasets, created a Google Cloud Storage (GCS) bucket with individual folders where the data files were uploaded.

- **state_GHG_emissions.docx**: Word doc containing information and descriptions regarding the state_GHG_emissions table.
- **us-climate-data-dict-v1.xlsx**: Data dictionary representing the entities and their attributes. Includes the source, GCS location and file type.
- **us-climate-erd-v1.pdf**: Entity Relationship Diagram (ERD) of the raw data. Visualization of how the different data would appear as tables and their relationships.

## Project 2
Two additional datasets were added to the project: one containing information on facilities that capture carbon from the atmosphere and another detailing the fuel share contributions to electricity generation by state. The carbon capture and policy datasets were originally in PDF format, so Google Gemini was used to extract and convert the unstructured data into structured JSON files that were uploaded into the GCS bucket. All the data files were then loaded into individual BigQuery tables that were stored within a raw dataset. The ERD was updated to reflect the new tables and their relationships. Following this, the data was checked to see that a given list of 10 validation criteria was met where each anomaly would be handled in the later projects. The criteria included addressing issues such as null values represented inconsistently, multiple attributes stored in a single field, lists of elements within individual cells, and mismatched identifier systems across tables.

- 1-us-climate-extract-ccus-facility.ipynb:
- 1-us-climate-extract-state-climate-policies.ipynb:
- 2-us-climate-data-load.ipynb:
- criteria-analysis.md:
- us-climate-data-dict-v2.xlsx:
- us-climate-erd-v2.pdf:

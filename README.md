# U.S.-Climate-Data-Warehouse
Collaborated with another student on building a cloud-based data warehouse on Google Cloud Platform (GCP).

## Project 1
Picked a domain, U.S. climate, and found datasets relating to that domain. Found 6 datasets that contained emission, temperature, policy, disaster and risk data. After collecting the datasets, created a Google Cloud Storage (GCS) bucket with individual folders where the data files were uploaded.

- **state_GHG_emissions.docx**: Contains information and descriptions regarding the state_GHG_emissions table.
- **us-climate-data-dict-v1.xlsx**: Data dictionary representing the entities and their attributes. Includes the source, GCS location and file type.
- **us-climate-erd-v1.pdf**: Entity Relationship Diagram (ERD) of the raw data. Visualization of how the different data would appear as tables and their relationships.

## Project 2
Two additional datasets were added to the project: one containing information on facilities that capture carbon from the atmosphere and another detailing the fuel share contributions to electricity generation by state. The carbon capture and policy datasets were originally in PDF format, so Google Gemini was used to extract and convert the unstructured data into structured JSON files that were uploaded into the GCS bucket. All the data files were then loaded into individual BigQuery tables that were stored within a raw dataset. The ERD was updated to reflect the new tables and their relationships. Following this, the data was checked to see that a given list of 10 validation criteria was met where each anomaly would be handled in the later projects. The criteria included addressing issues such as null values represented inconsistently, multiple attributes stored in a single field, lists of elements within individual cells, and mismatched identifier systems across tables.

- **1-us-climate-extract-ccus-facility.ipynb**: Extracts structured data from the carbon capture facility PDF using Google Gemini and uploads it to GCS.
- **1-us-climate-extract-state-climate-policies.ipynb**: Extracts structured data from 50 state climate policies PDFs using Google Gemini and uploads it to GCS.
- **2-us-climate-data-load.ipynb**: Loads data files from GCS into BigQuery tables, storing them in a raw dataset.
- **criteria-analysis.md**: Details where and how each validation criteria is met.
- **us-climate-data-dict-v2.xlsx**: Updated data dictionary with additional datasets.
- **us-climate-erd-v2.pdf**: Udpated ERD with additional tables.

## Project 3
A staging area was created in BigQuery to store the results of data transformations applied to resolve anomalies found in the raw dataset. These transformations included casting string fields to integers, replacing empty strings with proper null values, and splitting cells containing multiple embedded attributes into separate columns. In addition, irrelevant fields were removed, and select columns were renamed for clarity and consistency. The ERD was updated to reflect these changes, with modifications shown in bold

- **3-us-climate-stg-cleaned.ipynb**: Renderable version of the staging notebook for viewing directly on GitHub.
- **3-us-climate-stg.ipynb**: Performs data cleaning and transformation, writing the results to new BigQuery tables in the staging dataset.
- **us-climate-erd-stg.pdf**: Updated ERD showing changes made in the staging layer.

## Project 4
An intermediate area was created in BigQuery to store the results of data transformations applied to resolve the required remaining anomalies. These transformations included converting a list of embedded values in a single cell into an auxiliary table, creating a universal identifier, and decomposing a multi-entity table into separate tables.

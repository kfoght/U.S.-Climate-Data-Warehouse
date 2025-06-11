# U.S.-Climate-Data-Warehouse
Collaborated with another student on building a cloud-based data warehouse on Google Cloud Platform (GCP).

## Project 1
Picked a domain, U.S. climate, and found datasets relating to that domain. Found 6 datasets that contained emission, temperature, policy, disaster and risk data. After collecting the datasets, created a Google Cloud Storage (GCS) bucket with individual folders where the data files were uploaded.
- state_GHG_emissions.docx: Word doc containing information and descriptions regarding the state_GHG_emissions table.
- us-climate-data-dict-v1.xlsx: Data dictionary representing the entities and their attributes. Includes the source, GCS location and file type.
- us-climate-erd-v1.pdf: Entity Relationship Diagram (ERD) of the raw data. Visualization of how the different data would appear as tables and their relationships.

## Project 2
Found 2 more datasets that contained data regarding facilities that capture carbon from the atmosphere and which fuel shares contribute to electricity generation from each state. Two of the datasets were unstructered, the policy and carbon capture faciliies datasets were pdfs. Utilized Google Gemini to extract the data from the pdfs and store it as json files in the GCS bucket.

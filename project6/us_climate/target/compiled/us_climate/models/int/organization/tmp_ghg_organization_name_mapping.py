import json
import numpy
import vertexai
import pandas as pd
from vertexai.generative_models import GenerativeModel
from google.cloud import bigquery

# === Configurations ===
project_id = "kiaraerica"
region = "us-central1"
model_name = "gemini-2.0-flash-001"

# === Prompt ===
prompt = """Normalize the organization name and determine the country this organization belongs to.
For example, X and X LLC should be the same company X, Z glass and Z group should be the same company.
No abbreviations like "US" or "UK". Convert organizations like "N/A", "None", "null" all to null.

Return JSON in this format:
{
  "original_name": "Some Raw Org Name",
  "standardized_name": "<Standardized Organization Name>",
  "country": "<Country Name>"
}

No extra text or explanation. Only return valid JSON.
"""

# === Inference Function ===
def do_inference(row, model_instance):
    org = row.get("organization_name", "Unknown")
    combined_input = [f'organization_name="{org}"', prompt]
    print("combined_input:", combined_input)

    try:
        resp = model_instance.generate_content(combined_input)
        resp_text = resp.text.replace("```json", "").replace("```", "").strip()
        print("resp_text:", resp_text)
        return json.loads(resp_text)
    except Exception as e:
        print("Error while parsing json:", e, "From:", resp.text if 'resp' in locals() else 'N/A')
        return {
            "original_name": org,
            "standardized_name": None,
            "country": None
        }

# === Main dbt model function ===
def model(dbt, session):
    dbt.config(materialized="table")

    # Query manually created BigQuery table
    bq = bigquery.Client(project=project_id)
    query = """
        SELECT DISTINCT organization_name
        FROM `kiaraerica.dbt_us_climate_int.tmp_ghg_facilities_llm_org_checkpoint`
        WHERE organization_name IS NOT NULL
    """
    pandas_df = bq.query(query).to_dataframe()

    # Clean and deduplicate org names
    pandas_df = (
        pandas_df["organization_name"]
        .astype(str)
        .str.strip()
        .str.lower()
        .loc[lambda s: ~s.isin(["none", "n/a", "null", ""])]
        .drop_duplicates()
        .to_frame(name="organization_name")
    )

    # Init VertexAI
    vertexai.init(project=project_id, location=region)
    model_instance = GenerativeModel(model_name)

    combined_results = []
    for _, row in pandas_df.iterrows():
        result = do_inference(row, model_instance)
        combined_results.append(result)

    # Return DataFrame for dbt
    output_df = session.createDataFrame(combined_results)
    return output_df


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "kiaraerica"
    schema = "dbt_us_climate_int"
    identifier = "tmp_ghg_organization_name_mapping"
    
    def __repr__(self):
        return 'kiaraerica.dbt_us_climate_int.tmp_ghg_organization_name_mapping'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------



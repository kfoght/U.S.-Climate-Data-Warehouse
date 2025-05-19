import json
import numpy
import pandas
import vertexai
from vertexai.generative_models import GenerativeModel

# === Configurations ===
region = "us-central1"
model_name = "gemini-2.0-flash-001"

# === Prompt ===
prompt = """Normalize the organization name and determine the country this organization belongs to.
For example, X and X LLC should be the same company X, Z glass and Z group should be the same company.
Do not use abbreviations like "US" or "UK". Convert organizations like "N/A", "None", "null" all to null.

Return JSON in this format:
{
  "original_name": "Some Raw Org Name",
  "standardized_name": "<Standardized Organization Name>",
  "country": "<Country Name>"
}

No extra text or explanation. Only return valid JSON.
"""

# === Inference Function ===
def do_inference(rows):
    print("enter do_inference()")

    vertexai.init(location=region)
    model = GenerativeModel(model_name)

    results = []

    for _, row in rows.iterrows():
        org = row.get("organization_name", "Unknown")
        combined_input = [f'organization_name="{org}"', prompt]
        print("combined_input:", combined_input)

        try:
            resp = model.generate_content(combined_input)
            resp_text = resp.text.replace("```json", "").replace("```", "").strip()
            parsed = json.loads(resp_text)

            # Use original name if standardized name is missing or blank
            standardized = parsed.get("standardized_name", "").strip()
            if not standardized:
                standardized = org.strip()

            # Default country to empty string if missing
            country = parsed.get("country", "").strip()

            parsed = {
                "original_name": org,
                "standardized_name": standardized,
                "country": country
            }

        except Exception as e:
            print("Error parsing JSON:", e, "From:", resp.text if 'resp' in locals() else "No response")
            parsed = {
                "original_name": org,
                "standardized_name": org,
                "country": ""
            }

        results.append(parsed)

    return results



# === Main dbt model function ===
def model(dbt, session):
    dbt.config(materialized="table")

    # Use the preprocessed tmp_ccf_organizations
    orgs_df = dbt.ref("tmp_ccf_organizations").select("organization_name").distinct().toPandas()

    # Clean and filter
    orgs_df = (
        orgs_df["organization_name"]
        .astype(str)
        .str.strip()
        .loc[lambda s: ~s.isin(["none", "n/a", "null", "unknown", ""])]
        .drop_duplicates()
        .to_frame(name="organization_name")
    )

    # Batch the inputs
    batch_size = 10
    num_batches = max(1, int(len(orgs_df) / batch_size))
    batches = numpy.array_split(orgs_df, num_batches)

    # Init Vertex AI and run LLM inference
    combined_results = []
    for batch in batches:
        results = do_inference(batch)
        combined_results.extend(results)

    # Return DataFrame for dbt
    return session.createDataFrame(combined_results)


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"tmp_ccf_organizations": "kiaraerica.dbt_us_climate_int.tmp_ccf_organizations"}
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
    identifier = "tmp_ccf_organization_name_mapping"
    
    def __repr__(self):
        return 'kiaraerica.dbt_us_climate_int.tmp_ccf_organization_name_mapping'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------



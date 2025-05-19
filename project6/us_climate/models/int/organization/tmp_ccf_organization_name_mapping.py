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

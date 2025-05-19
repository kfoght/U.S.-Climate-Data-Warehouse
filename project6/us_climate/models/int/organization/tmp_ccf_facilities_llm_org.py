import json
import numpy
import vertexai
from vertexai.generative_models import GenerativeModel

# Configurations
region = "us-central1"
model_name = "gemini-2.0-flash-001"

# Prompt template without Jinja syntax (using single curly braces)
prompt_template = """Given a carbon capture facility record with:
ccf_facility_name, city, state, category, status, industry.

Identify the organization that owns or operates this facility, or return null if unknown.
Return EXACTLY one JSON line:
{"ccf_id": 123456, "organization_name": "Some Organization Name"}
No extra text or explanation.
"""

def do_inference(rows):
    print("enter do_inference()")

    vertexai.init(location=region)
    model = GenerativeModel(model_name)

    results = []

    for _, row in rows.iterrows():
        row_text = (
            f"ccf_id={row.get('ccf_id', 'Unknown')}, "
            f"ccf_facility_name='{row.get('ccf_facility_name', 'Unknown')}', "
            f"city='{row.get('city', 'Unknown')}', "
            f"state='{row.get('state', 'Unknown')}', "
            f"category='{row.get('category', 'Unknown')}', "
            f"status='{row.get('status', 'Unknown')}', "
            f"industry='{row.get('industry', 'Unknown')}'"
        )

        combined_prompt = row_text + "\n" + prompt_template
        try:
            resp = model.generate_content(combined_prompt)
            resp_text = resp.text.replace("json", "").replace("", "").strip()
            parsed = json.loads(resp_text)
        except Exception as e:
            print("Error parsing JSON:", e, "From:", resp.text)
            parsed = {
                "ccf_id": row.get("ccf_id", None),
                "organization_name": None
            }

        results.append(parsed)

    return results

def model(dbt, session):
    # Get input table from dbt
    input_df = dbt.ref("tmp_ccf_facilities_no_org")

    # Convert to a pandas DataFrame
    pandas_df = input_df.select(
        "ccf_id",
        "ccf_facility_name",
        "city",
        "state",
        "category",
        "status",
        "industry"
    ).toPandas()

    # Batch records for inference
    batch_size = 10
    num_batches = max(1, int(len(pandas_df) / batch_size))
    batches = numpy.array_split(pandas_df, num_batches)

    combined_results = []
    for batch in batches:
        results = do_inference(batch)
        combined_results.extend(results)

    # Convert the results back to a Spark/BigQuery-compatible DataFrame
    output_df = session.createDataFrame(combined_results)

    return output_df
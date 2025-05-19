import json
import vertexai
from vertexai.generative_models import GenerativeModel
from pyspark.sql.types import StructField, StructType, StringType

region = "us-central1"
model_name = "gemini-2.0-flash-001"
prompt = """For each place abbreviation provided, return the full name and its capital.
Focus on U.S. states, U.S. districts, U.S. territories, and Canadian provinces.
If an abbreviation does not correspond to one of these categories—for example, if it is "National" or "USTERR"—return null for both the full name and the capital.
If you can't find name, return null.
If you can't find capital, return null.
Format the answer as a list of dictionaries with the schema:
{"geo_id": "<abbreviation>", "name": "<full name>", "capital": "<capital>"}
Example:
[{"geo_id": "TX", "full_name": "Texas", "capital": "Austin"},
 {"geo_id": "ON", "full_name": "Ontario", "capital": "Toronto"}
"""

def model(dbt, session):
    geo_ref_df = dbt.ref("tmp_geo_references")

    num_geo_ref = geo_ref_df.count()
    print("num geo ref:", num_geo_ref)

    geo_ref_str = geo_ref_df.toPandas().to_string(header=False)

    vertexai.init(location=region)
    model = GenerativeModel(model_name=model_name)
    resp = model.generate_content([geo_ref_str, prompt])
    resp_text = resp.text.replace("```json", "").replace("```", "").replace("\n", "")
    print("results_raw:", resp_text)

    try:
        replacements = json.loads(resp_text)
        print("replacements:", replacements)
    except Exception as e:
        print("Error while parsing json:", e, ". The error was caused by:", resp_text)
        return {}

    schema = StructType([
        StructField("geo_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("capital", StringType(), True),
    ])

    output_df = session.createDataFrame(replacements, schema)

    return output_df
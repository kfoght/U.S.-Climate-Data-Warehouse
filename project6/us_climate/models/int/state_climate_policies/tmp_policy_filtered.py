import json
import vertexai
from vertexai.generative_models import GenerativeModel
from pyspark.sql.types import StructField, StructType, StringType

region = "us-central1"
model_name = "gemini-2.0-flash-001"
prompt = """For each policy entry, standardize the `policy_area` and `category` values while keeping `policy` unchanged.

Standardization rules:
1. If `policy_area` is "Climate" or "Climate Governance and Equity", standardize it to "Climate Governance and Equity".
2. If `category` is "Environmental Justice" or "Environmental Justice and Equity", standardize it to "Environmental Justice and Equity".
3. If `policy_area` is "Buildings and Efficiency" and the `category` is "Building Efficiency" standardize `category` to "Building Efficiency and Standards".
4. If `policy_area` is "Buildings and Efficiency" and the `category` is "Building Standards" standardize `category` to "Building Efficiency and Standards".
4. If `policy_area` is "Electricity" and `category` is "Transmission, Distribution, and Energy Storage" keep values unchanged.
5. If `policy_area` is "Transmission, Distribution, and Energy Storage" and `category` is "Electricity" swap there values so that:
   - `policy_area` becomes "Electricity"
   - `category` becomes "Transmission, Distribution, and Energy Storage"
6. If no standardization is needed, keep the values unchanged.

Format the answer as a list of dictionaries with the schema:
{"policy": "<policy>", "policy_area": "<standardized policy_area>", "category": "<standardized category>"}

Example:
[{"policy": "Just Transition Advisory Bodies	", "policy_area": "Climate Governance and Equity	", "category": "Just Transition"},
 {"policy": "Interconnection Standards", "policy_area": "Electricity", "category": "Transmission, Distribution, and Energy Storage"}]
"""

def model(dbt, session):
    policy_df = dbt.ref("tmp_policy")

    num_policy = policy_df.count()
    print("num policy:", num_policy)

    policy_str = policy_df.toPandas().to_string(header=False)

    vertexai.init(location=region)
    model = GenerativeModel(model_name=model_name)
    resp = model.generate_content([policy_str, prompt])
    resp_text = resp.text.replace("```json", "").replace("```", "").replace("\n", "")
    print("results_raw:", resp_text)

    try:
        replacements = json.loads(resp_text)
        print("replacements:", replacements)
    except Exception as e:
        print("Error while parsing json:", e, ". The error was caused by:", resp_text)
        return {}

    schema = StructType([
        StructField("policy", StringType(), True),
        StructField("policy_area", StringType(), True),
        StructField("category", StringType(), True),
    ])

    output_df = session.createDataFrame(replacements, schema)

    return output_df
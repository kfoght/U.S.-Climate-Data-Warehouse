import json
import numpy
import pandas as pd
import vertexai
from vertexai.generative_models import GenerativeModel
from google.cloud import bigquery

project_id = "kiaraerica"
region = "us-central1"
model_name = "gemini-2.0-flash-001"
checkpoint_table = "dbt_us_climate_int.tmp_ghg_facilities_llm_org_checkpoint"
full_checkpoint_table = f"{project_id}.{checkpoint_table}"

prompt = """Given a facility record from the GHG emissions dataset with:
facility_id, facility_name, city, state, naics_code, industry_sector1, industry_sector2, industry_sector3.

Identify the organization that owns or operates this facility, or return null if unknown.
Return EXACTLY one JSON line:
{"facility_id": "SAMPLE_ID", "organization_name": "Some Organization Name"}
No extra text or explanation.
"""

def do_inference(batch):
    print("Running inference on a batch...")
    vertexai.init(project=project_id, location=region)
    model = GenerativeModel(model_name)

    results = []
    for _, row in batch.iterrows():
        row_text = (
            f"facility_id={row['facility_id']}, "
            f"facility_name='{row['facility_name']}', "
            f"city='{row['city']}', "
            f"state='{row['state']}', "
            f"naics_code='{row['naics_code']}', "
            f"industry_sector1='{row['industry_sector1']}', "
            f"industry_sector2='{row['industry_sector2']}', "
            f"industry_sector3='{row['industry_sector3']}'"
        )
        full_prompt = row_text + "\n" + prompt

        try:
            resp = model.generate_content(full_prompt)
            text = resp.text.replace("```json", "").replace("```", "").strip()
            parsed = json.loads(text)
        except Exception as e:
            print("Error:", e, "Prompt:", row_text)
            parsed = {
                "facility_id": row["facility_id"],
                "organization_name": None
            }

        results.append(parsed)
    return results

def model(dbt, session):
    bq = bigquery.Client(project=project_id)

    checkpoint_schema = [
        bigquery.SchemaField("facility_id", "STRING"),
        bigquery.SchemaField("organization_name", "STRING"),
    ]

    try:
        bq.get_table(full_checkpoint_table)
        print("Checkpoint table already exists.")
    except Exception as e:
        print("Checkpoint table not found. Creating...")
        table = bigquery.Table(full_checkpoint_table, schema=checkpoint_schema)
        bq.create_table(table)
        print("Checkpoint table created.")

    checkpoint_query = f"""
    SELECT DISTINCT facility_id
    FROM `{full_checkpoint_table}`
    """
    processed_df = bq.query(checkpoint_query).to_dataframe()
    processed_ids = processed_df["facility_id"].astype(str).tolist()

    input_df = dbt.ref("tmp_ghg_facilities_no_org")
    pandas_df = input_df.select(
        "facility_id", "facility_name", "city", "state",
        "naics_code", "industry_sector1", "industry_sector2", "industry_sector3"
    ).distinct().toPandas()

    pandas_df["facility_id"] = pandas_df["facility_id"].astype(str)
    unprocessed_df = pandas_df[~pandas_df["facility_id"].isin(processed_ids)]

    print(f"Records remaining: {len(unprocessed_df)}")

    batch_size = 10
    batches = numpy.array_split(unprocessed_df, max(1, len(unprocessed_df) // batch_size))

    all_results = []
    for batch in batches:
        results = do_inference(batch)
        all_results.extend(results)

        if results:
            checkpoint_df = pd.DataFrame(results)
            bq.load_table_from_dataframe(
                checkpoint_df,
                full_checkpoint_table,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            ).result()
            print(f"Checkpointed {len(checkpoint_df)} rows.")

    final_df = pd.DataFrame(all_results).drop_duplicates(subset=["facility_id"])
    return session.createDataFrame(final_df)
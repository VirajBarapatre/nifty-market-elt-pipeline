"""Milestone 1 smoke test: can we authenticate to BigQuery and create the raw dataset?

Run from the repo root with the venv active:
    python scripts/check_bq_connection.py
"""
import os

from dotenv import load_dotenv
from google.cloud import bigquery

# Read .env into environment variables (does nothing if they're already set, e.g. in Docker).
load_dotenv()

project_id = os.environ["GCP_PROJECT_ID"]
location = os.environ["BQ_LOCATION"]
raw_dataset = os.environ["BQ_RAW_DATASET"]

# The client finds the key file through GOOGLE_APPLICATION_CREDENTIALS automatically.
client = bigquery.Client(project=project_id, location=location)

# 1. A query that scans no table data, so it uses none of the free 1 TiB.
result = client.query("SELECT 1 AS ok").result()
print("Query ok:", [row.ok for row in result])

# 2. Create the raw dataset if missing. exists_ok=True makes this safe to rerun (idempotent).
dataset = bigquery.Dataset(f"{project_id}.{raw_dataset}")
dataset.location = location
client.create_dataset(dataset, exists_ok=True)

print("Datasets:", [d.dataset_id for d in client.list_datasets()])

"""Audit tables in BigQuery raw: a quarantine for rejected rows and a log of every run.

Both are APPEND-only load jobs (allowed in the sandbox, unlike INSERT/MERGE).
They are logs: re-running a date adds new rows with a new run_id instead of
replacing old ones, so you can see every attempt. Filter by run_id to see one run.
"""
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Sandbox tables are deleted 60 days after creation and a write does NOT reset that
# clock, so every write pushes the expiry forward. Shared with load.py.
EXPIRY_DAYS = 59

# Rejected rows can be broken in any way, so every price column is FLOAT64 and NULLABLE
# (a null or fractional volume must still fit, or we'd lose the evidence).
REJECTED_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("trade_date", "DATE"),
    bigquery.SchemaField("open", "FLOAT64"),
    bigquery.SchemaField("high", "FLOAT64"),
    bigquery.SchemaField("low", "FLOAT64"),
    bigquery.SchemaField("close", "FLOAT64"),
    bigquery.SchemaField("adj_close", "FLOAT64"),
    bigquery.SchemaField("volume", "FLOAT64"),
    bigquery.SchemaField("reject_reason", "STRING"),
    bigquery.SchemaField("rejected_at", "TIMESTAMP"),
]

RUN_LOG_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("trade_date", "DATE"),
    bigquery.SchemaField("status", "STRING"),         # loaded / loaded_with_warnings / blocked
    bigquery.SchemaField("rows_fetched", "INT64"),
    bigquery.SchemaField("rows_good", "INT64"),
    bigquery.SchemaField("rows_rejected", "INT64"),
    bigquery.SchemaField("rows_loaded", "INT64"),
    bigquery.SchemaField("reject_rate", "FLOAT64"),
    bigquery.SchemaField("null_rate", "FLOAT64"),
    bigquery.SchemaField("tickers_expected", "INT64"),
    bigquery.SchemaField("tickers_received", "INT64"),
    bigquery.SchemaField("missing_tickers", "STRING"),
    bigquery.SchemaField("problems", "STRING"),       # gate messages joined with " | "
    bigquery.SchemaField("logged_at", "TIMESTAMP"),
]


def extend_expiry(client: bigquery.Client, table_id: str) -> bigquery.Table:
    """Push the table's expiry EXPIRY_DAYS into the future and return the updated table."""
    table = client.get_table(table_id)
    table.expires = datetime.now(timezone.utc) + timedelta(days=EXPIRY_DAYS)
    return client.update_table(table, ["expires"])


def append_rows(df: pd.DataFrame, client: bigquery.Client, table_id: str,
                schema: list[bigquery.SchemaField]) -> int:
    """Append df to table_id with a load job (creating the table if needed)."""
    if df.empty:
        return 0
    client.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)
    config = bigquery.LoadJobConfig(schema=schema,
                                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    columns = [f.name for f in schema]
    client.load_table_from_dataframe(df[columns], table_id, job_config=config).result()
    extend_expiry(client, table_id)
    logger.info("Appended %d rows to %s", len(df), table_id)
    return len(df)


def write_rejected(rejected: pd.DataFrame, run_id: str, client: bigquery.Client,
                   project: str, dataset: str, table: str = "rejected_prices") -> int:
    """Quarantine: keep every rejected row with why it was rejected and which run saw it."""
    if rejected.empty:
        return 0
    df = rejected.copy()
    df["run_id"] = run_id
    df["rejected_at"] = pd.Timestamp.now(tz="UTC")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("float64")
    return append_rows(df, client, f"{project}.{dataset}.{table}", REJECTED_SCHEMA)


def write_run_log(row: dict, client: bigquery.Client, project: str, dataset: str,
                  table: str = "pipeline_runs") -> None:
    """One row per run attempt: what came in, what was rejected, and the verdict."""
    df = pd.DataFrame([{**row, "logged_at": pd.Timestamp.now(tz="UTC")}])
    append_rows(df, client, f"{project}.{dataset}.{table}", RUN_LOG_SCHEMA)

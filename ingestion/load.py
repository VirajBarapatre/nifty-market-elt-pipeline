"""Load validated prices into BigQuery raw.daily_prices, idempotently.

The BigQuery sandbox does not allow DML (MERGE / INSERT / UPDATE / DELETE),
so we upsert in two steps that are both allowed:

  1. LOAD JOB:  write the new batch to raw.daily_prices_incoming (replace it).
  2. QUERY JOB: SELECT old rows that are NOT in the batch, UNION ALL the batch,
                and write the result over raw.daily_prices (WRITE_TRUNCATE).

Running it twice for the same dates replaces those rows instead of adding
duplicates. That is what idempotent means here.
"""
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Explicit schema: we decide the types, BigQuery does not guess them.
# All NULLABLE because validate.py already guarantees no nulls, and a query
# job's output columns are always NULLABLE, so REQUIRED would clash on rewrite.
SCHEMA = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("trade_date", "DATE"),
    bigquery.SchemaField("open", "FLOAT64"),
    bigquery.SchemaField("high", "FLOAT64"),
    bigquery.SchemaField("low", "FLOAT64"),
    bigquery.SchemaField("close", "FLOAT64"),
    bigquery.SchemaField("adj_close", "FLOAT64"),
    bigquery.SchemaField("volume", "INT64"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]
COLUMNS = [field.name for field in SCHEMA]

# Sandbox tables are deleted 60 days after creation, and rewriting a table does
# NOT reset that clock. We push the expiry forward on every run instead.
EXPIRY_DAYS = 59


def load_prices(df: pd.DataFrame, client: bigquery.Client, project: str, dataset: str,
                table: str = "daily_prices") -> int:
    """Upsert df into project.dataset.table on (ticker, trade_date). Returns rows loaded."""
    if df.empty:
        logger.warning("Nothing to load: the batch is empty")
        return 0

    target = f"{project}.{dataset}.{table}"
    incoming = f"{target}_incoming"

    # Step 0: make sure the target exists, so the first run's query has something to read.
    client.create_table(bigquery.Table(target, schema=SCHEMA), exists_ok=True)

    # Step 1: load the batch into the incoming table, replacing whatever was there.
    load_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(df[COLUMNS], incoming, job_config=load_config).result()
    logger.info("Loaded %d rows into %s", len(df), incoming)

    # Step 2: rebuild the target = (old rows not in this batch) + (this batch).
    cols = ", ".join(COLUMNS)
    sql = f"""
        SELECT {cols}
        FROM `{target}` AS t
        WHERE NOT EXISTS (
            SELECT 1 FROM `{incoming}` AS i
            WHERE i.ticker = t.ticker AND i.trade_date = t.trade_date
        )
        UNION ALL
        SELECT {cols} FROM `{incoming}`
    """
    query_config = bigquery.QueryJobConfig(
        destination=target,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.query(sql, job_config=query_config).result()

    # Step 3: keep the table alive. A pipeline that runs regularly never expires;
    # one that stops for ~2 months loses its raw data (and must be backfilled again).
    table = client.get_table(target)
    table.expires = datetime.now(timezone.utc) + timedelta(days=EXPIRY_DAYS)
    table = client.update_table(table, ["expires"])
    logger.info("Upserted %d rows; %s now has %d rows, expires %s",
                len(df), target, table.num_rows, table.expires)
    return len(df)

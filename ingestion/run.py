"""Run fetch -> validate -> quality gates -> load for a date range, from the command line.

Daily run (default: re-fetch the last 7 days, safe because the load is idempotent):
    python -m ingestion.run
Backfill:
    python -m ingestion.run --start 2023-11-27 --end 2026-11-25
Load a CSV instead of calling yfinance (used to demo the quality gates on a bad file):
    python -m ingestion.run --input-file tests/fixtures/bad_prices.csv

Exit codes: 0 = loaded and clean, 1 = blocked by a hard gate (nothing loaded),
2 = loaded but a soft gate fired (e.g. missing tickers). Schedulers read exit codes.
"""
import argparse
import logging
import os
import sys
import uuid
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from ingestion.audit import write_rejected, write_run_log
from ingestion.fetch import fetch_prices
from ingestion.load import load_prices
from ingestion.quality import compute_metrics, hard_gate_failures, run_log_row, soft_gate_failures
from ingestion.tickers import load_tickers
from ingestion.validate import validate

logger = logging.getLogger("ingestion.run")


def read_input_file(path: str) -> pd.DataFrame:
    """Read a prices CSV shaped like fetch_prices() output (ingested_at is added if absent)."""
    df = pd.read_csv(path)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    if "ingested_at" not in df.columns:
        df["ingested_at"] = pd.Timestamp.now(tz="UTC")
    return df


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch, validate and load Nifty prices")
    parser.add_argument("--start", type=date.fromisoformat, default=date.today() - timedelta(days=7))
    # yfinance's end is exclusive, so default to tomorrow to include today.
    parser.add_argument("--end", type=date.fromisoformat, default=date.today() + timedelta(days=1))
    # Default: every symbol in the official Nifty 50 list (dbt/seeds/ind_nifty50list.csv).
    parser.add_argument("--tickers", nargs="+", default=None,
                        help="Override, e.g. --tickers RELIANCE.NS TCS.NS")
    parser.add_argument("--input-file", default=None,
                        help="Read prices from this CSV instead of yfinance")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_dotenv()
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    client = bigquery.Client(project=project, location=os.environ["BQ_LOCATION"])
    run_id = f"cli__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}__{uuid.uuid4().hex[:6]}"

    expected = load_tickers()                 # the universe: anything else is unknown_ticker
    tickers = args.tickers or expected
    if args.input_file:
        logger.info("Run %s: reading %s", run_id, args.input_file)
        prices = read_input_file(args.input_file)
    else:
        logger.info("Run %s: %s to %s (end exclusive), %d tickers",
                    run_id, args.start, args.end, len(tickers))
        prices = fetch_prices(tickers, args.start, args.end)

    good, rejected = validate(prices, allowed_tickers=expected)
    write_rejected(rejected, run_id, client, project, dataset)

    # Completeness is judged on the latest date in the batch (from the CLI there is no
    # "run date"). With --tickers we only expect the tickers we asked for.
    trade_date = max(prices["trade_date"]) if not prices.empty else args.end - timedelta(days=1)
    metrics = compute_metrics(run_id, trade_date, prices, good, rejected, tickers)
    logger.info("Metrics: %s", metrics.as_row())

    hard = hard_gate_failures(metrics)
    if hard:
        write_run_log(run_log_row(metrics, "blocked", hard), client, project, dataset)
        logger.error("ALERT: load BLOCKED, nothing written to daily_prices: %s", "; ".join(hard))
        return 1

    loaded = load_prices(good, client, project, dataset)
    soft = soft_gate_failures(metrics)
    status = "loaded_with_warnings" if soft else "loaded"
    write_run_log(run_log_row(metrics, status, soft, loaded), client, project, dataset)
    if soft:
        logger.error("ALERT: loaded %d rows, but: %s", loaded, "; ".join(soft))
        return 2
    logger.info("Loaded %d rows, all gates passed", loaded)
    return 0


if __name__ == "__main__":
    sys.exit(main())

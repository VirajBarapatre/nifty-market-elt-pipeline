"""Run fetch -> validate -> load for a date range.

Daily run (default: re-fetch the last 7 days, safe because the load is idempotent):
    python -m ingestion.run
Backfill:
    python -m ingestion.run --start 2023-11-27 --end 2026-11-25
"""
import argparse
import logging
import os
from datetime import date, timedelta

from dotenv import load_dotenv
from google.cloud import bigquery

from ingestion.fetch import fetch_prices
from ingestion.load import load_prices
from ingestion.tickers import load_tickers
from ingestion.validate import validate

logger = logging.getLogger("ingestion.run")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch, validate and load Nifty prices")
    parser.add_argument("--start", type=date.fromisoformat, default=date.today() - timedelta(days=7))
    # yfinance's end is exclusive, so default to tomorrow to include today.
    parser.add_argument("--end", type=date.fromisoformat, default=date.today() + timedelta(days=1))
    # Default: every symbol in the official Nifty 50 list (dbt/seeds/ind_nifty50list.csv).
    parser.add_argument("--tickers", nargs="+", default=None,
                        help="Override, e.g. --tickers RELIANCE.NS TCS.NS")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_dotenv()
    project = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client(project=project, location=os.environ["BQ_LOCATION"])

    tickers = args.tickers or load_tickers()
    logger.info("Run for %s to %s (end exclusive), %d tickers", args.start, args.end, len(tickers))
    prices = fetch_prices(tickers, args.start, args.end)
    good, rejected = validate(prices)
    if not rejected.empty:
        # Milestone 6 writes these to a quarantine table; for now we log them.
        logger.warning("Rejected %d rows:\n%s", len(rejected),
                       rejected[["ticker", "trade_date", "reject_reason"]].to_string())

    load_prices(good, client, project, os.environ["BQ_RAW_DATASET"])


if __name__ == "__main__":
    main()

"""Fetch daily OHLCV prices for NSE tickers from yfinance.

yfinance is an unofficial wrapper around Yahoo Finance: it can rate limit us,
return nothing for a bad ticker, or fail for a moment. So we fetch one ticker
at a time, retry with backoff, and log every ticker we could not get.
"""
import logging
import time
from datetime import date, datetime, timezone

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# Column names in the order the raw BigQuery table will use.
OUTPUT_COLUMNS = [
    "ticker", "trade_date", "open", "high", "low", "close", "adj_close", "volume", "ingested_at",
]

# yfinance's column names -> our snake_case names.
RENAME_MAP = {
    "Open": "open", "High": "high", "Low": "low", "Close": "close",
    "Adj Close": "adj_close", "Volume": "volume",
}


def fetch_one_ticker(ticker: str, start: date, end: date,
                     max_retries: int = 3, backoff_seconds: float = 2.0) -> pd.DataFrame:
    """Download one ticker. Returns an empty DataFrame if every attempt fails."""
    for attempt in range(1, max_retries + 1):
        try:
            raw = yf.download(
                ticker,
                start=start,
                end=end,                 # yfinance treats `end` as EXCLUSIVE
                interval="1d",
                auto_adjust=False,       # keep the real traded prices AND Adj Close
                actions=False,           # no dividend/split columns
                multi_level_index=False, # flat columns for a single ticker
                progress=False,
                threads=False,
            )
            if raw.empty:
                # Bad ticker, a range with no trading days, or a silent rate limit.
                logger.warning("No rows for %s (%s to %s), attempt %d/%d",
                               ticker, start, end, attempt, max_retries)
            else:
                df = raw.rename(columns=RENAME_MAP).reset_index()
                df["trade_date"] = pd.to_datetime(df["Date"]).dt.date
                df["ticker"] = ticker
                return df
        except Exception as exc:  # network error, rate limit, parsing error...
            logger.warning("Error fetching %s, attempt %d/%d: %s", ticker, attempt, max_retries, exc)

        if attempt < max_retries:
            time.sleep(backoff_seconds * 2 ** (attempt - 1))  # 2s, 4s, 8s...

    logger.error("Giving up on %s after %d attempts", ticker, max_retries)
    return pd.DataFrame()


def fetch_prices(tickers: list[str], start: date, end: date,
                 pause_seconds: float = 0.5) -> pd.DataFrame:
    """Fetch all tickers for [start, end) and return one long DataFrame.

    Every row is one ticker on one trading day. A failed ticker is logged
    and skipped, so one bad symbol does not kill the whole run.
    """
    frames = []
    failed = []
    for ticker in tickers:
        df = fetch_one_ticker(ticker, start, end)
        if df.empty:
            failed.append(ticker)
        else:
            frames.append(df)
        time.sleep(pause_seconds)  # be polite to an unofficial API

    if failed:
        logger.error("Failed tickers (%d/%d): %s", len(failed), len(tickers), failed)

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    prices = pd.concat(frames, ignore_index=True)
    prices["ingested_at"] = datetime.now(timezone.utc)
    logger.info("Fetched %d rows for %d tickers", len(prices), len(tickers) - len(failed))
    return prices[OUTPUT_COLUMNS]


if __name__ == "__main__":
    # Quick manual check: python -m ingestion.fetch
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    sample = fetch_prices(
        ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS"],
        start=date(2026, 11, 1),
        end=date.today(),
    )
    print(sample.head(10))
    print(sample.dtypes)

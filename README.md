# Nifty Market Analytics Pipeline

An automated ELT pipeline that loads daily OHLCV prices for Nifty 50 stocks into BigQuery, transforms them with dbt into tested analytics tables, orchestrates the runs with Airflow, and serves a Power BI dashboard.

> **Disclaimer:** This is a data engineering portfolio project. Nothing in this repository, its tables or its dashboard is investment advice or a recommendation to buy or sell any security. The data comes from an unofficial source and may be incomplete or wrong.

## Architecture

_Diagram coming in Milestone 8._

```
yfinance (NSE tickers) -> Python ingestion -> BigQuery raw -> dbt staging -> dbt marts -> Power BI
                               ^ orchestrated daily by Airflow (Docker Compose) ^
```

## Tech stack

Python 3.11 · Airflow (Docker Compose) · Google BigQuery (free sandbox) · dbt-core + dbt-bigquery · pytest · GitHub Actions · Power BI Desktop

## Data layers

| Layer | BigQuery dataset | What it holds |
|---|---|---|
| Raw | `raw` | Prices exactly as yfinance returned them |
| Staging | `analytics` (stg_ models) | Cleaned, renamed, typed, bad rows removed |
| Marts | `analytics` (fact/dim models) | Business-ready facts, dimensions and metrics for the dashboard |

## Setup

1. Install Python 3.11 and Docker Desktop.
2. Create a virtual env: `py -3.11 -m venv .venv`, then `.venv\Scripts\Activate.ps1`.
3. Install packages: `pip install -r requirements.txt`.
4. Create a BigQuery sandbox project and a service account with **BigQuery Data Editor** and **BigQuery Job User**. Save its JSON key **outside** this repo.
5. Copy `.env.example` to `.env` and fill in your values.
6. Check the connection: `python scripts/check_bq_connection.py`.

## Design decisions

_Filled in as each milestone lands._

## Cost notes

Runs entirely on the BigQuery sandbox (no billing account): 10 GiB storage, 1 TiB of queries per month.

## Known limitations

- **yfinance is unofficial.** It scrapes Yahoo Finance, can be rate limited, and can return missing days or bad tickers. The pipeline retries and logs these, but the data is not guaranteed.
- **BigQuery sandbox limits.** No DML (MERGE, UPDATE, DELETE) and tables expire after 60 days, so loads are designed around load jobs and history must be re-loaded after expiry.

## What this does NOT prove

_Filled in at Milestone 8: no tick data, not for trading, no cloud-scale load, unofficial data source._

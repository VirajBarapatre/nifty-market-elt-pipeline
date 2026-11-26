"""Daily Nifty 50 pipeline: is_trading_day -> fetch -> validate -> load -> dbt_run -> dbt_test.

Airflow concepts used here (see PROGRESS.md for longer explanations):
- DAG: the whole workflow; a graph of tasks with no cycles, plus a schedule.
- Task: one node in the DAG. Operator: the template a task is built from
  (@task = PythonOperator, BashOperator runs a shell command).
- XCom: small values passed between tasks, stored in Airflow's metadata DB.
  We pass FILE PATHS, never DataFrames.
- logical_date: the date a run is "for". Every task derives its trade date from it,
  so re-running or backfilling a date gives the same result (idempotency).
"""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, get_current_context, task

log = logging.getLogger(__name__)

IST = pendulum.timezone("Asia/Kolkata")
DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data"))
LOOKBACK_DAYS = 7  # re-fetch a week each run; safe because the load is idempotent

DBT = "/home/airflow/dbt_venv/bin/dbt"
# Write dbt's compiled files and logs inside the container, not into the mounted repo.
DBT_FLAGS = "--project-dir /opt/airflow/project/dbt --target-path /tmp/dbt_target --log-path /tmp/dbt_logs"


def run_trade_date() -> date:
    """The market date this run is for, taken from the run's logical date in IST."""
    ctx = get_current_context()
    # Scheduled/backfill runs have a logical_date. In Airflow 3 a manual trigger without
    # --logical-date has none, so fall back to when the run was requested (run_after).
    when = ctx.get("logical_date") or ctx["dag_run"].run_after
    return pendulum.instance(when).in_timezone(IST).date()


@dag(
    dag_id="nifty_daily_pipeline",
    description="Fetch Nifty 50 prices, validate, load to BigQuery raw, build dbt marts.",
    schedule="0 17 * * 1-5",              # 17:00 IST, Mon-Fri: after the 15:30 close
    start_date=pendulum.datetime(2026, 11, 1, tz=IST),
    catchup=False,                        # don't auto-run every missed day since start_date
    max_active_runs=1,                    # the load rewrites a table: never two runs at once
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["nifty", "elt"],
    # Demo switch (trigger with conf {"simulate_load_failure": true}) to show retries
    # and that a failed task blocks everything downstream. Off for scheduled runs.
    params={"simulate_load_failure": Param(False, type="boolean")},
)
def nifty_daily_pipeline():

    @task.short_circuit
    def is_trading_day() -> bool:
        """Return False on weekends and NSE holidays; Airflow then SKIPS all downstream tasks."""
        import yfinance as yf

        d = run_trade_date()
        if d.weekday() >= 5:
            log.info("%s is a weekend, skipping", d)
            return False
        # If the Nifty 50 index itself has no bar for this date, the market was closed.
        index = yf.download("^NSEI", start=d, end=d + timedelta(days=1),
                            progress=False, auto_adjust=False, multi_level_index=False)
        is_open = not index.empty
        log.info("%s trading day: %s", d, is_open)
        return is_open

    @task
    def fetch() -> str:
        from ingestion.fetch import fetch_prices
        from ingestion.tickers import load_tickers

        d = run_trade_date()
        start, end = d - timedelta(days=LOOKBACK_DAYS), d + timedelta(days=1)  # end is exclusive
        prices = fetch_prices(load_tickers(), start, end)
        if prices.empty:
            raise ValueError(f"yfinance returned no rows for {start}..{d}")  # fail -> retry
        out = DATA_DIR / d.isoformat() / "prices.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        prices.to_parquet(out, index=False)
        log.info("Wrote %d rows to %s", len(prices), out)
        return str(out)                   # XCom: a short string, not the data

    @task
    def validate(prices_path: str) -> str:
        import pandas as pd

        from ingestion.validate import validate as validate_df

        good, rejected = validate_df(pd.read_parquet(prices_path))
        folder = Path(prices_path).parent
        good.to_parquet(folder / "good.parquet", index=False)
        rejected.to_parquet(folder / "rejected.parquet", index=False)  # quarantine in Milestone 6
        log.info("Validated: %d good, %d rejected", len(good), len(rejected))
        if not rejected.empty:
            log.warning("Rejected rows:\n%s",
                        rejected[["ticker", "trade_date", "reject_reason"]].to_string())
        if good.empty:
            raise ValueError("Every row failed validation")
        return str(folder / "good.parquet")

    @task
    def load(good_path: str) -> int:
        import pandas as pd
        from google.cloud import bigquery

        from ingestion.load import load_prices

        if get_current_context()["params"]["simulate_load_failure"]:
            raise RuntimeError("Simulated load failure (demo): downstream tasks must not run")

        project = os.environ["GCP_PROJECT_ID"]
        client = bigquery.Client(project=project, location=os.environ["BQ_LOCATION"])
        return load_prices(pd.read_parquet(good_path), client, project, os.environ["BQ_RAW_DATASET"])

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} seed {DBT_FLAGS} && {DBT} run {DBT_FLAGS}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_FLAGS}",
    )

    # Dependencies: each task runs only if the one before it succeeded (trigger rule all_success).
    gate = is_trading_day()
    prices_path = fetch()                 # calling a @task returns an XComArg (a future value)
    good_path = validate(prices_path)     # passing it in creates the fetch >> validate edge
    loaded = load(good_path)

    gate >> prices_path                   # no data passes here, so set the order explicitly
    loaded >> dbt_run >> dbt_test


nifty_daily_pipeline()

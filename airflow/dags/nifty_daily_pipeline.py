"""Daily Nifty 50 pipeline.

is_trading_day -> fetch -> quality_gate -> load -> dbt_run -> dbt_test
                                             +-> record_run

Airflow concepts used here (see PROGRESS.md for longer explanations):
- DAG: the whole workflow; a graph of tasks with no cycles, plus a schedule.
- Task: one node in the DAG. Operator: the template a task is built from
  (@task = PythonOperator, BashOperator runs a shell command).
- XCom: small values passed between tasks, stored in Airflow's metadata DB.
  We pass FILE PATHS and small dicts, never DataFrames.
- logical_date: the date a run is "for". Every task derives its trade date from it,
  so re-running or backfilling a date gives the same result (idempotency).

Data quality (see ingestion/quality.py):
- HARD gate (quality_gate): too many rejected rows or nulls -> AirflowFailException.
  Nothing is loaded, and Airflow does NOT retry, because retrying can't fix bad data.
- SOFT gate (record_run): rows loaded but tickers missing -> record_run fails as an
  alert, while dbt still builds from the rows we did get.
- Every run, blocked or not, writes raw.rejected_prices and raw.pipeline_runs.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.exceptions import AirflowFailException
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
    """The market date this run is for, in IST.

    Scheduled and backfill runs have a logical_date. A manual trigger in Airflow 3
    may not, so fall back to when the run was requested (run_after).
    """
    ctx = get_current_context()
    when = ctx.get("logical_date") or ctx["dag_run"].run_after
    return pendulum.instance(when).in_timezone(IST).date()


def bq():
    """BigQuery client plus the names every task needs."""
    from google.cloud import bigquery

    project = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client(project=project, location=os.environ["BQ_LOCATION"])
    return client, project, os.environ["BQ_RAW_DATASET"]


def alert_on_failure(context) -> None:
    """on_failure_callback: one greppable ALERT line per failed task.

    On a laptop the alert is a log line. In a real job this is where you'd post to
    Slack or email; the rest of the pipeline wouldn't change.
    """
    ti = context["task_instance"]
    log.error("ALERT dag=%s task=%s run=%s try=%s error=%s",
              ti.dag_id, ti.task_id, ti.run_id, ti.try_number, context.get("exception"))


@dag(
    dag_id="nifty_daily_pipeline",
    description="Fetch Nifty 50 prices, quality-gate them, load to BigQuery raw, build dbt marts.",
    schedule="0 17 * * 1-5",              # 17:00 IST, Mon-Fri: after the 15:30 close
    start_date=pendulum.datetime(2026, 11, 1, tz=IST),
    catchup=False,                        # don't auto-run every missed day since start_date
    max_active_runs=1,                    # the load rewrites a table: never two runs at once
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "on_failure_callback": alert_on_failure,
    },
    params={
        # Demo switches for a manual trigger ("Trigger DAG w/ config"). Both off by default.
        "simulate_load_failure": Param(False, type="boolean",
                                       description="Make the load task fail, to see retries."),
        "input_file": Param("", type="string",
                            description="CSV under airflow/data/ to use instead of yfinance, "
                                        "e.g. bad_prices.csv. Empty = fetch from yfinance."),
    },
    tags=["nifty", "elt"],
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
        from ingestion.run import read_input_file
        from ingestion.tickers import load_tickers

        d = run_trade_date()
        params = get_current_context()["params"]
        if params.get("input_file"):
            prices = read_input_file(DATA_DIR / params["input_file"])
            log.info("Read %d rows from %s instead of yfinance", len(prices), params["input_file"])
        else:
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
    def quality_gate(prices_path: str) -> dict:
        """Validate, quarantine rejects, measure the batch, and block it if it's bad."""
        import pandas as pd

        from ingestion.audit import write_rejected, write_run_log
        from ingestion.quality import compute_metrics, hard_gate_failures, run_log_row
        from ingestion.tickers import load_tickers
        from ingestion.validate import validate

        run_id = get_current_context()["run_id"]
        expected = load_tickers()
        prices = pd.read_parquet(prices_path)
        good, rejected = validate(prices, allowed_tickers=expected)
        folder = Path(prices_path).parent
        good.to_parquet(folder / "good.parquet", index=False)

        client, project, dataset = bq()
        write_rejected(rejected, run_id, client, project, dataset)   # quarantine
        if not rejected.empty:
            log.warning("Quarantined %d rows:\n%s", len(rejected),
                        rejected[["ticker", "trade_date", "reject_reason"]].to_string())

        metrics = compute_metrics(run_id, run_trade_date(), prices, good, rejected, expected)
        log.info("Metrics: %s", metrics.as_row())

        hard = hard_gate_failures(metrics)
        if hard:
            write_run_log(run_log_row(metrics, "blocked", hard), client, project, dataset)
            # AirflowFailException = fail now, skip the retries: bad data stays bad.
            raise AirflowFailException("Load BLOCKED by quality gate: " + "; ".join(hard))

        # JSON round trip turns the date into a string, so the dict is XCom-safe.
        return {"good_path": str(folder / "good.parquet"),
                "metrics": json.loads(json.dumps(metrics.as_row(), default=str))}

    @task
    def load(checked: dict) -> int:
        import pandas as pd

        from ingestion.load import load_prices

        if get_current_context()["params"].get("simulate_load_failure"):
            raise RuntimeError("Simulated load failure (param simulate_load_failure=true)")
        client, project, dataset = bq()
        return load_prices(pd.read_parquet(checked["good_path"]), client, project, dataset)

    @task
    def record_run(checked: dict, rows_loaded: int) -> None:
        """Write the run log, then raise the soft-gate alert if tickers are missing."""
        from ingestion.audit import write_run_log
        from ingestion.quality import RunMetrics, run_log_row, soft_gate_failures

        row = dict(checked["metrics"])
        row["trade_date"] = date.fromisoformat(row["trade_date"])
        row["missing_tickers"] = [t for t in row["missing_tickers"].split(",") if t]
        metrics = RunMetrics(**row)

        soft = soft_gate_failures(metrics)
        status = "loaded_with_warnings" if soft else "loaded"
        client, project, dataset = bq()
        write_run_log(run_log_row(metrics, status, soft, rows_loaded), client, project, dataset)
        if soft:
            raise AirflowFailException("Loaded, but " + "; ".join(soft))

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} seed {DBT_FLAGS} && {DBT} run {DBT_FLAGS}",
    )

    # Tests (including "every ticker present on the latest day"), then source freshness:
    # raw.daily_prices.ingested_at older than 5 days fails the task = stale-data alert.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_FLAGS} && {DBT} source freshness {DBT_FLAGS}",
    )

    # Dependencies: each task runs only if the one before it succeeded (trigger rule all_success).
    gate = is_trading_day()
    prices_path = fetch()                 # calling a @task returns an XComArg (a future value)
    checked = quality_gate(prices_path)   # passing it in creates the fetch >> quality_gate edge
    loaded = load(checked)

    gate >> prices_path                   # no data passes here, so set the order explicitly
    loaded >> dbt_run >> dbt_test
    record_run(checked, loaded)           # runs beside dbt: a soft alert doesn't stop the marts


nifty_daily_pipeline()

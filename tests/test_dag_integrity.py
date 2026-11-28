"""DAG integrity tests: does the DAG import, and is it wired the way we think?

Needs Apache Airflow installed, so it is SKIPPED in the normal test run (requirements.txt
has no Airflow) and runs in its own CI job, which installs Airflow 3.4.0. No database,
scheduler or BigQuery is needed: parsing the file is enough.
"""
from pathlib import Path

import pytest

# Not just "airflow": the repo's own airflow/ folder would import as an empty namespace package.
pytest.importorskip("airflow.sdk")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAG_FOLDER = Path(__file__).resolve().parents[1] / "airflow" / "dags"
DAG_ID = "nifty_daily_pipeline"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAG_FOLDER))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_has_expected_tasks(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert {t.task_id for t in dag.tasks} == {
        "is_trading_day", "fetch", "quality_gate", "load", "record_run", "dbt_run", "dbt_test",
    }


def test_quality_gate_runs_before_load_and_dbt(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert "quality_gate" in dag.get_task("load").upstream_task_ids
    assert "load" in dag.get_task("dbt_run").upstream_task_ids
    assert "dbt_run" in dag.get_task("dbt_test").upstream_task_ids


def test_safety_settings(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert dag.max_active_runs == 1          # the load rewrites a table: never two at once
    assert dag.catchup is False
    assert dag.get_task("fetch").retries == 2

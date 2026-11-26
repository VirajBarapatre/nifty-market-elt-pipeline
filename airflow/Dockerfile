# Custom Airflow image: official Airflow + our pipeline's Python libraries,
# with dbt in its OWN virtual env so its pins can't clash with Airflow's.
FROM apache/airflow:3.4.0-python3.11

USER airflow

# Pipeline libraries go into Airflow's environment. Re-pinning apache-airflow stops
# pip from silently upgrading/downgrading Airflow while resolving our packages.
COPY airflow/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==3.4.0" -r /tmp/requirements-airflow.txt

# dbt in a separate venv; the DAG calls /home/airflow/dbt_venv/bin/dbt.
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-core==1.12.*" "dbt-bigquery==1.12.*"

# No secrets here: the service account key is mounted at runtime, read-only.

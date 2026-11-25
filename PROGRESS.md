# Progress

## Milestone 1: Setup (done, 2026-11-25)

### What was done
- Repo structure: `ingestion/`, `tests/`, `dbt/`, `airflow/dags`, `airflow/plugins`, `scripts/`, `docs/images/`, `.github/workflows/`.
- `.gitignore` blocking secrets (`.env`, key file patterns), the venv, Python caches, dbt and Airflow generated output, and local data.
- `.env.example` (committed, placeholders only) and `.env` (gitignored, real values).
- Python 3.11.9 venv in `.venv`; packages: google-cloud-bigquery 3.47.0, python-dotenv 1.2.4, pytest 8.4.2.
- BigQuery sandbox project `nifty-pipeline-viraj` (no billing). Service account `nifty-pipeline-sa` with BigQuery Data Editor + BigQuery Job User. Key stored in `C:\Users\viraj\.gcp\`, outside the repo.
- `scripts/check_bq_connection.py` passed: `SELECT 1` ran and the `raw` dataset was created in `asia-south1`.
- `git init`; confirmed `.env` and `.venv/` are ignored. Nothing committed yet.
- README skeleton with the not-investment-advice disclaimer and known limitations.

### Decisions and why
- **Python 3.11 pinned** (machine default is 3.14): the same version locally, in the Airflow image and in CI avoids "works on my machine" bugs.
- **Key outside the repo + gitignore patterns**: two layers of protection. A key pushed to git stays in history even after deletion, so it would have to be revoked.
- **Least privilege roles** instead of Owner/Editor: the pipeline can only read/write tables and run jobs.
- **Location asia-south1 (Mumbai)**: all datasets must share one location to be queried together.
- **BigQuery sandbox limits** (no DML, 60-day table expiry, 10 GiB storage): idempotent loads in Milestone 2 must use load jobs, not MERGE.
- **Raw / staging / marts layers**: raw keeps untouched source data so transforms can be rebuilt without calling the API again; staging cleans; marts serve business-ready tables.

### What I still don't understand
- Nothing yet.

### Interview questions
- Deferred to the end of the project. See the question bank (kept outside the repo).

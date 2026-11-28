# Progress

A milestone-by-milestone build log: what was done, the decisions behind it, and what went wrong. Later work sometimes changed an earlier decision; where it did, the earlier entry points to the change.

## Milestone 1: Setup (done, 2026-11-25)

### What was done
- Repo structure: `ingestion/`, `tests/`, `dbt/`, `airflow/dags`, `airflow/plugins`, `scripts/`, `docs/images/`, `.github/workflows/`.
- `.gitignore` blocking secrets (`.env`, key file patterns), the venv, Python caches, dbt and Airflow generated output, and local data.
- `.env.example` (committed, placeholders only) and `.env` (gitignored, real values).
- Python 3.11.9 venv in `.venv`; packages: google-cloud-bigquery 3.47.0, python-dotenv 1.2.4, pytest 8.4.2.
- BigQuery sandbox project `nifty-pipeline-viraj` (no billing). A dedicated service account with only BigQuery Data Editor + BigQuery Job User; its key is stored in the user profile, outside the repo.
- `scripts/check_bq_connection.py` passed: `SELECT 1` ran and the `raw` dataset was created in `asia-south1`.
- `git init`; confirmed `.env` and `.venv/` are ignored before the first commit.
- README skeleton with the not-investment-advice disclaimer and known limitations.

### Decisions and why
- **Python 3.11 pinned** (machine default is 3.14): the same version locally, in the Airflow image and in CI avoids "works on my machine" bugs.
- **Key outside the repo + gitignore patterns**: two layers of protection. A key pushed to git stays in history even after deletion, so it would have to be revoked.
- **Least privilege roles** instead of Owner/Editor: the pipeline can only read/write tables and run jobs.
- **Location asia-south1 (Mumbai)**: all datasets must share one location to be queried together.
- **BigQuery sandbox limits** (no DML, 60-day table expiry, 10 GiB storage): idempotent loads in Milestone 2 must use load jobs, not MERGE.
- **Raw / staging / marts layers**: raw keeps untouched source data so transforms can be rebuilt without calling the API again; staging cleans; marts serve business-ready tables.

## Milestone 2: Ingestion (done, 2026-11-25)

### What was done
- `ingestion/fetch.py`: one ticker at a time from yfinance, 3 retries with exponential backoff, failed tickers logged and skipped.
- `ingestion/validate.py`: schema check (whole batch fails), then row checks: nulls, prices <= 0, negative/zero volume, high < low, open/close outside [low, high], duplicate ticker+date. Rejected rows are kept with a `reject_reason`.
- `ingestion/load.py`: idempotent upsert without DML: load job into `raw.daily_prices_incoming` (WRITE_TRUNCATE), then a query job rewrites `raw.daily_prices` as old rows not in the batch UNION ALL the batch. Pushes the table expiry forward 59 days on every run (later reduced to 57, see Milestone 7).
- `ingestion/tickers.py`: reads symbols from the official NSE list `dbt/seeds/ind_nifty50list.csv` (niftyindices.com).
- `ingestion/run.py`: fetch -> validate -> load. Default re-fetches the last 7 days; `--start/--end` for backfills.
- Tested idempotency: loaded the same dates twice, 80 rows both times, 80 unique keys.
- 3-year backfill (2023-11-27 to 2026-11-24): 36,900 fetched, 106 rejected (all zero_volume, mostly placeholder rows on 15 Jan 2026 and 18 Mar 2025), 36,799 rows in `raw.daily_prices`, 50 tickers, no duplicates. Took about 66 seconds.
- pytest: 20 tests (validate, fetch with a fake yfinance, tickers). All pass.

### Decisions and why
- **auto_adjust=False, keep both close and adj_close**: raw close matches NSE for reconciliation; adj_close is for returns so splits/dividends don't look like crashes. The raw layer keeps everything.
- **One ticker per request**: a bad symbol fails alone, retries are simple.
- **No MERGE**: the sandbox blocks DML, so we use a load job plus a query job with a destination table (both allowed). The rewrite is atomic.
- **No date partitioning**: in the sandbox, partitions older than 60 days would expire immediately and wipe the backfill.
- **Rewrite the whole table each run**: fine at ~37k rows; at large scale you'd use partitions and MERGE on a paid project.
- **7-day lookback on daily runs**: late or intraday rows get corrected on the next run because the load is idempotent.
- **Reject zero-volume rows**: Yahoo returns placeholder rows on non-trading days.
- **Ticker list from the official NSE CSV, kept in dbt/seeds**: one source of truth for ingestion and dbt.

### Known caveats
- Survivorship bias: today's Nifty 50 list is used for all 3 years.
- Rows fetched before 15:30 IST are intraday; the next run fixes them.
- If the pipeline stops for ~2 months, the sandbox deletes the raw table and a backfill is needed.

## Milestone 3: dbt staging (done, 2026-11-25)

### What was done
- dbt-core 1.12 + dbt-bigquery 1.12 project in `dbt/`. `profiles.yml` is committed but holds no secrets: every value is an `env_var()`. Run locally with `dotenv -f ..\.env run -- dbt <command> --profiles-dir .`.
- Seed: the official NSE list `dbt/seeds/ind_nifty50list.csv` loaded to `analytics_seeds.ind_nifty50list`.
- Source `raw.daily_prices` with freshness on `ingested_at`: warn after 3 days, error after 5.
- `stg_daily_prices` (view): renamed and typed columns, `symbol` without `.NS`, `price_id` = ticker + date, filters bad rows again, dedupes on ticker+date keeping the latest `ingested_at`.
- `stg_stocks` (view): symbol, ticker, company name, sector (NSE "Industry"), series, ISIN.
- 15 tests: not_null on key columns, unique on `price_id` and on symbol/ticker, relationships from price tickers to `stg_stocks`, accepted_values on series = EQ.
- `dbt build`: 18/18 passed (1 seed, 2 views, 15 tests). `dbt source freshness`: pass.

### Decisions and why
- **Staging = views**: no storage cost, always reflects raw; marts will be tables because the dashboard reads them often.
- **Custom schemas** (`analytics_staging`, `analytics_seeds`, later `analytics_marts`): layers are visible as separate datasets.
- **Surrogate key `price_id` + unique test** instead of a package for multi-column uniqueness: no extra dependency, and the key is reusable in marts.
- **Re-filter and dedupe in staging**: staging must not trust whatever loaded the raw table.
- **relationships instead of accepted_values for tickers**: the list stays in one place (the seed) and updates when the index changes; accepted_values is used on `series` instead.
- **Freshness 3/5 days**: weekends and market holidays would make a 1-day threshold warn every Monday.

## Milestone 4: dbt marts (done, 2026-11-26)

### What was done
- Star schema in `analytics_marts`: `fact_daily_prices` (grain: ticker + trading day), `dim_stock` (50 rows, company and sector), `dim_date` (one row per calendar day, with is_weekend and is_trading_day).
- `fact_daily_prices` is "incremental by rewrite": existing rows older than 10 days (read from `{{ this }}`) + the last 10 days from staging, written with CREATE OR REPLACE TABLE AS SELECT. Verified: the second run took the incremental branch and kept 36,799 rows with 36,799 unique keys.
- `mart_stock_metrics`: daily return (LAG on adjusted close), 20/50-day moving averages, 30-day annualised volatility (stddev x sqrt(252)); windows use ROWS, so they count trading days; values are NULL until the full window exists.
- `mart_top_movers`: top 5 gainers and losers per day with RANK().
- `mart_sector_daily`: equal-weighted average return per sector per day, advancers and decliners.
- 37 tests (keys, relationships fact -> dims, accepted_values) plus a warn-only singular test for daily moves over 25%. `dbt build`: 45 pass, 1 warn. `dbt docs generate` produces the catalog and lineage graph.
- Re-ran ingestion for 18-25 Nov: 25 Nov now has closing prices for 43 stocks; Yahoo returned null values for 7 (APOLLOHOSP, BAJAJ-AUTO, BHARTIARTL, HCLTECH, MARUTI, POWERGRID, SBILIFE). The next daily run's 7-day re-fetch should fill them in.

### Decisions and why
- **Incremental by rewrite instead of dbt's incremental materialization**: every BigQuery incremental strategy uses MERGE, which the sandbox blocks. CTAS is DDL and is allowed. Downside: the whole table is rewritten each run; rows older than 10 days need `--full-refresh` to change.
- **10-day lookback**: catches late corrections from yfinance and the 7-day re-fetch in ingestion.
- **Metric marts as full tables**: rolling windows need 50 days of history anyway, and 37k rows rebuild in seconds.
- **Adjusted close for returns and MAs**: splits and dividends don't show as crashes.
- **dim_date covers calendar days**: Power BI date slicers have no gaps; is_trading_day separates holidays.
- **Equal-weighted sectors**: simple and explainable; not the same as NSE's market-cap-weighted sector indices.

### Known data issues
- The plausibility test flagged TMPV.NS -40% on 2025-10-14 and TRENT.NS -33% on 2026-01-01. Both were investigated in Milestone 6: the Tata Motors demerger (Yahoo never adjusts demergers) and a Yahoo split-adjustment error. They affected returns, volatility, top movers and sector averages on those days until Milestone 6 handled them.
- The 7 stocks Yahoo left null for 25 Nov were filled by the first scheduled Airflow run's 7-day re-fetch (Milestone 5).

## Milestone 5: Airflow orchestration (done, 2026-11-26)

### What was done
- Airflow 3.4.0 in Docker Compose (`airflow/`), adapted from the official compose file: Postgres metadata DB, API server (UI on :8080), scheduler, DAG processor; LocalExecutor.
- Custom image `nifty-airflow:3.4.0`: official `apache/airflow:3.4.0-python3.11` + yfinance, pandas, BigQuery client; dbt-core/dbt-bigquery 1.12 in a separate venv (`/home/airflow/dbt_venv`).
- The service account key is bind-mounted read-only at `/opt/airflow/secrets/gcp-sa.json`; never copied into the image. `GCP_KEY_FILE_HOST` and `AIRFLOW_JWT_SECRET` live in `.env`.
- DAG `nifty_daily_pipeline`: is_trading_day -> fetch -> validate -> load -> dbt_run -> dbt_test. Schedule `0 17 * * 1-5` (17:00 IST weekdays), catchup=False, max_active_runs=1, 2 retries with exponential backoff.
- First scheduled run (26 Nov): all 6 tasks succeeded in under 2 minutes; the 7-day re-fetch also filled the 7 stocks Yahoo had left null for 25 Nov.
- Backfill 23-25 Nov: 23 and 25 succeeded with no duplicate rows (row count unchanged); 24 Nov (holiday) ran is_trading_day = False and skipped every downstream task.
- Failure demo (trigger with conf `{"simulate_load_failure": true}`): load failed on all 3 attempts, dbt_run and dbt_test were upstream_failed and never ran, nothing reached BigQuery.

### Airflow concepts (in my words)
- **DAG**: a workflow as a directed graph of tasks with no cycles, plus a schedule.
- **Task / operator**: a task is one step; an operator is the template it's built from (`@task` = Python, `BashOperator` = shell command). A task instance is one task for one run.
- **XCom**: small values passed between tasks, stored in the metadata DB. We pass file paths, never DataFrames.
- **logical_date**: the date a run is for. Tasks derive the trading date from it, so reruns/backfills are reproducible. Manual triggers in Airflow 3 have no logical date, so we fall back to `run_after`.
- **catchup**: with catchup=False, unpausing creates only the latest run, not every missed one since start_date. Backfills are explicit (`airflow backfill create`).
- **Short-circuit**: a task that returns False skips everything downstream (holidays show as skipped, not failed).
- **Trigger rule**: default all_success, so a failed task blocks all downstream tasks (upstream_failed).
- **Idempotency**: every task gives the same result if rerun for the same date (fixed date window + upsert load), so retries and backfills are safe.

### Decisions and why
- **LocalExecutor**: one machine; Celery + Redis add moving parts with no benefit here.
- **dbt in its own venv**: dbt and Airflow pin conflicting libraries.
- **is_trading_day via the Nifty index bar**: no hand-maintained holiday list to go stale.
- **max_active_runs=1 (and --max-active-runs 1 for backfills)**: the load rewrites the raw table; parallel runs would race.

### Bugs found and fixed
- Manual trigger without a logical date crashed with KeyError 'logical_date' (Airflow 3 behaviour). Fixed by falling back to `dag_run.run_after`.
- `backfill create --to-date 2026-11-25` didn't include 25 Nov: the 17:00 IST (11:30 UTC) schedule time is after midnight of the to-date. Use the next day as to-date.
- Backfill runs ran in parallel despite max_active_runs=1 on the DAG; pass `--max-active-runs 1` to backfill.

## Milestone 6: Data quality and monitoring (done, 2026-11-27)

### What was done
- `ingestion/validate.py`: new rule 7, `unknown_ticker` (a ticker not in the Nifty 50 list).
- `ingestion/quality.py` (pure pandas, unit tested): per-run metrics (rows fetched/good/rejected, reject rate, null rate over price cells, tickers received vs expected on the trade date) and two kinds of gate:
  - HARD (block the load): no rows, reject rate > 10%, null rate > 5%.
  - SOFT (load, then alert): any expected ticker missing on the trade date.
- `ingestion/audit.py`: two append-only audit tables in `raw`, written with load jobs (no DML):
  - `raw.rejected_prices`: the quarantine. Every rejected row with run_id, reject_reason, rejected_at.
  - `raw.pipeline_runs`: one row per run attempt with the metrics, status (loaded / loaded_with_warnings / blocked) and the gate messages.
  - `extend_expiry()` moved here and is shared with load.py, so `daily_prices`, `rejected_prices` and `pipeline_runs` push their 60-day sandbox expiry forward on every write. (`daily_prices_incoming` is rewritten by every load and recreated if it ever expires.)
- `ingestion/run.py`: `--input-file` to load a CSV instead of yfinance; runs the gates; exit codes 0 clean, 1 blocked, 2 loaded with warnings.
- Airflow DAG: `validate` became `quality_gate` (validate, quarantine, metrics, hard gate). New `record_run` task beside dbt writes the run log and raises the soft alert. `dbt_test` now also runs `dbt source freshness` (stale data alert). `on_failure_callback` logs one `ALERT ...` line per failed task. New param `input_file` for demos.
- dbt: seed `known_corporate_actions` (TMPV demerger 2025-10-14, TRENT split adjustment error 2026-01-01). `mart_stock_metrics` sets daily_return to NULL on those days and adds `is_corporate_action_day`. New singular test `assert_all_tickers_on_latest_day` (severity error).
- Investigation: TMPV -40% = Tata Motors demerger (Yahoo never adjusts demergers). TRENT -33% = Yahoo applied a 3-for-2 split (x1.5) only from 1 Jan 2026 while its own actions list the split on 4 Jun 2026; the real move that day was about +0.4%.
- Bad-file demo (`tests/fixtures/bad_prices.csv`, 8 rows, 1 good): CLI exit 1 with "ALERT: load BLOCKED ... reject rate 87.5% > 10%". In Airflow, quality_gate failed on attempt 1 with no retry and every downstream task was upstream_failed. Both times 7 rows went to the quarantine, a "blocked" row went to pipeline_runs, and raw.daily_prices stayed at 36,894 rows.
- Normal run: 240 rows, 0 rejected, 50 of 50 tickers, all 7 tasks green. dbt build 53 PASS, 0 WARN (the plausibility test now passes). pytest 29 passed.

### Decisions and why
- **Fail closed on hard gates**: if most of a batch is bad, the source or file is broken, and the "good" rows can't be trusted either. Loading nothing is safer than loading half.
- **Missing tickers are soft**: the rows we got are fine, and the 7-day re-fetch usually fills the gap next run. Blocking would hide 49 good stocks because of 1 missing one.
- **AirflowFailException for hard gates**: retries fix network blips, not bad data. Normal exceptions (yfinance down) still retry.
- **record_run runs beside dbt, not before it**: a soft alert shouldn't stop the marts from building.
- **Append-only audit tables**: they are logs; re-running a date adds a new attempt with a new run_id instead of hiding the old one. (Trade-off: a retry after a write can duplicate rows; filter by run_id.)
- **Quarantine columns all FLOAT64 and nullable**: broken rows must still fit, or the evidence is lost.
- **Null the return on corporate-action days instead of deleting the row**: the prices are real; only the return across the gap is meaningless.
- **Completeness checked twice**: Python checks the batch before load; dbt checks the warehouse after, and would catch a ticker lost between raw and marts.

### Known caveats
- Moving averages and volatility around the TMPV and TRENT dates are still distorted (prices before and after are on different scales). A proper fix is a per-ticker adjustment factor applied in staging. Volatility is NULL for 30 trading days after each action day because that window has a NULL return.
- The TRENT split date is uncertain (Yahoo shows two dates); confirm on NSE's corporate actions page.
- Airflow runs with `input_file` measure completeness on the run date, not the file's date. Harmless for the demo (the hard gate blocks first).
- Alerts are log lines only; on a real job the callback would post to Slack or email.

## Milestone 7: Power BI dashboard (done, 2026-11-27)

### What was done
- Power BI Desktop connected to BigQuery with the built-in Google BigQuery connector (Google account sign-in, no key file), Import mode.
- Loaded the 6 marts plus `raw.pipeline_runs` and `raw.rejected_prices`.
- Rebuilt the model as a star schema: auto-detect had joined the fact and marts to each other on price_id with bidirectional filters, so all relationships were deleted and recreated as 7 many-to-one, single-direction joins from `dim_stock` (ticker) and `dim_date` (date_day). Auto-detect and Auto date/time turned off, `dim_date` marked as the date table, `month_key` added to sort months.
- Measures in a `_Measures` table: Latest Date, Market Date, Latest Close, Day Return %, Period Return %, Advancers, Decliners, Latest Volatility %, Times Top Gainer, and per-stock Adj Close, MA 20, MA 50, Daily Return %, Volatility 30d %, Volume (blank unless one stock is selected).
- 3 pages: Market overview, Stock deep-dive, Pipeline health (run log times converted from UTC to IST).
- Saved as `powerbi/nifty_dashboard.pbix`; screenshots in `docs/images/` and in the README.
- Same evening: the scheduled run failed because the sandbox suddenly refused a 59-day table expiry (58 still worked; clocks checked, not the cause). Fixed: expiry is now 57 days and the expiry update is best effort (logs ALERT, never fails the load). Cleared only the failed tasks of that run; it finished green and Power BI page 3 shows it. The quarantine table's expiry is now also extended on runs with no rejected rows, so its history can't quietly expire.

### Decisions and why
- **Import, not DirectQuery**: about 37k rows is tiny, visuals are instant, and each refresh is a handful of cheap queries inside the free 1 TiB per month.
- **Star schema with single-direction filters**: filters flow from dimensions to facts only, so every filter has exactly one path and no relationship is ambiguous.
- **Separate measures table**: measures are easy to find and don't depend on any one fact table.
- **`VAR d = [Market Date]` before `CALCULATE`**: the date is evaluated once, in the outer filter context, then used as a filter.
- **`REMOVEFILTERS(dim_stock)` in Market Date**: "today" means the market's latest day, not a stock's last row, so a missing stock can't show a stale return.
- **`AVERAGEX(VALUES(dim_stock[ticker]), ...)` for period return**: computes each stock's return (context transition), then averages them equally, matching `mart_sector_daily`.
- **HASONEVALUE guards**: price-level measures are blank when more than one stock is in context, because summing share prices is meaningless.
- **Pipeline health page**: shows the monitoring from the previous milestone to a business user, not just in logs.

### Known caveats
- Equal-weighted returns, not the official free-float weighted Nifty 50.
- Survivorship bias: using today's constituents for all 3 years flatters long-period returns (58% over 3 years).
- TMPV and TRENT price levels jump on their corporate-action days, which distorts period return and moving averages across those dates; volatility is blank for 30 trading days after each.
- The .pbix holds a snapshot and is refreshed by hand (scheduling needs Power BI Service, which needs a work or paid account).

## Milestone 8: CI and final polish (done, 2026-11-28)

### What was done
- `.github/workflows/ci.yml`: three jobs on every push to main and every pull request: Python unit tests, `dbt parse` with placeholder settings, and Airflow DAG integrity (Airflow 3.4.0 installed with the official constraints file).
- `tests/test_dag_integrity.py`: the DAG imports with no errors, has the 7 expected tasks, quality_gate runs before load and dbt, max_active_runs is 1, catchup is off, retries are 2. Skips when Airflow isn't installed.
- DAG now imports AirflowFailException from `airflow.sdk.exceptions` (the old path is deprecated in 3.4).
- Worked like a team: branch `add-ci`, pull request, all 3 checks green, then a deliberately broken dbt ref turned dbt parse red while the other jobs stayed green, reverted with `git revert`, green again, squash merged.
- README: CI badge, Testing and CI section. MIT LICENSE added.
- README completed: architecture diagram (Mermaid), data layers per BigQuery dataset, design decisions and an honest "what this does not prove" section; milestone references removed from code comments.
- Secrets audit of the whole git history: no .env, key file or private key was ever committed.

### Decisions and why
- **No credentials in CI**: tests use fakes and parse-only checks, so the service account key never leaves the laptop and CI can't run up cost.
- **dbt parse, not dbt build**: build needs BigQuery; parse still catches the most common breakages (refs, Jinja, YAML) for free.
- **DAG tests in their own job**: Airflow is heavy and has its own pinned dependencies, so it gets a separate install with the official constraints file instead of polluting requirements.txt.
- **`importorskip("airflow.sdk")`**: the repo's own `airflow/` folder imports as an empty namespace package, so checking plain `airflow` would wrongly think Airflow is installed.
- **Revert instead of rewriting history**: a new commit that undoes the bad one keeps the evidence and never rewrites shared history.
- **Squash merge**: main gets one clean commit per change; the pull request keeps the detailed history.

### Known caveats
- CI can't catch SQL that is only invalid inside BigQuery, or yfinance changing its API.
- Older commit messages still contain milestone numbers; history on main is not rewritten.

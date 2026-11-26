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

## Milestone 2: Ingestion (done, 2026-11-25)

### What was done
- `ingestion/fetch.py`: one ticker at a time from yfinance, 3 retries with exponential backoff, failed tickers logged and skipped.
- `ingestion/validate.py`: schema check (whole batch fails), then row checks: nulls, prices <= 0, negative/zero volume, high < low, open/close outside [low, high], duplicate ticker+date. Rejected rows are kept with a `reject_reason`.
- `ingestion/load.py`: idempotent upsert without DML: load job into `raw.daily_prices_incoming` (WRITE_TRUNCATE), then a query job rewrites `raw.daily_prices` as old rows not in the batch UNION ALL the batch. Pushes the table expiry forward 59 days on every run.
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

### What I still don't understand
- Nothing yet.

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

### What I still don't understand
- Nothing yet.

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
- The plausibility test flags TMPV.NS -40% on 2025-10-14 (likely the Tata Motors demerger, which Yahoo's adjusted close does not adjust for) and TRENT.NS -33% on 2026-01-01 (not yet investigated). Both affect returns, volatility, top movers and sector averages on those days.
- Re-ran ingestion for 18-25 Nov: 25 Nov now has closing prices for 43 stocks; Yahoo returned null values for 7 (APOLLOHOSP, BAJAJ-AUTO, BHARTIARTL, HCLTECH, MARUTI, POWERGRID, SBILIFE). The next daily run's 7-day re-fetch should fill them in.

### What I still don't understand
- Nothing yet.

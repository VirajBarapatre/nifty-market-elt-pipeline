-- Fact table: one row per ticker per trading day.
--
-- "Incremental by rewrite". dbt's incremental strategies on BigQuery all use MERGE,
-- which the BigQuery sandbox blocks. Instead, each run rebuilds the table as:
--     existing rows older than the lookback window   (read from {{ this }})
--   + the last N days re-read from staging            (picks up late fixes)
-- That is CREATE OR REPLACE TABLE ... AS SELECT (DDL), which the sandbox allows.
-- Run with --full-refresh to rebuild everything from staging (e.g. after a backfill).

{{ config(materialized='table') }}

{% set lookback_days = 10 %}
{% set existing = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) %}
{% set rebuild_all = flags.FULL_REFRESH or existing is none %}

with new_rows as (

    select
        price_id,
        ticker,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        adj_close_price,
        volume,
        ingested_at
    from {{ ref('stg_daily_prices') }}
    {% if not rebuild_all %}
    where trade_date >= date_sub(current_date('Asia/Kolkata'), interval {{ lookback_days }} day)
    {% endif %}

)

{% if rebuild_all %}

select * from new_rows

{% else %}

, kept_rows as (

    select
        price_id, ticker, trade_date, open_price, high_price, low_price,
        close_price, adj_close_price, volume, ingested_at
    from {{ this }}
    where trade_date < date_sub(current_date('Asia/Kolkata'), interval {{ lookback_days }} day)

)

select * from kept_rows
union all
select * from new_rows

{% endif %}

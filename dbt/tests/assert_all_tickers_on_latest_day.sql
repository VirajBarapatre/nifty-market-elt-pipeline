-- Singular test: every Nifty 50 stock must have a row on the latest trading day.
-- Returns one row per missing ticker; any row fails the test (severity error).
-- This is the warehouse-side twin of the Python soft gate: it also catches a
-- ticker that went missing between raw and the marts.
{{ config(severity='error') }}

with latest as (

    select max(trade_date) as trade_date
    from {{ ref('fact_daily_prices') }}

),

present as (

    select distinct f.ticker
    from {{ ref('fact_daily_prices') }} as f
    join latest using (trade_date)

)

select s.ticker, (select trade_date from latest) as missing_on
from {{ ref('dim_stock') }} as s
left join present as p using (ticker)
where p.ticker is null

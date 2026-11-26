-- One row per calendar day across the price history, with a flag for trading days.
-- Calendar days (not just trading days) so Power BI date slicers have no gaps.

with bounds as (

    select min(trade_date) as first_day, max(trade_date) as last_day
    from {{ ref('stg_daily_prices') }}

),

calendar as (

    select date_day
    from bounds, unnest(generate_date_array(first_day, last_day)) as date_day

),

trading_days as (

    select distinct trade_date
    from {{ ref('stg_daily_prices') }}

)

select
    c.date_day,
    extract(year from c.date_day)                  as year,
    extract(quarter from c.date_day)               as quarter,
    extract(month from c.date_day)                 as month,
    format_date('%b %Y', c.date_day)               as month_label,
    format_date('%A', c.date_day)                  as day_name,
    extract(dayofweek from c.date_day) in (1, 7)   as is_weekend,     -- BigQuery: 1 = Sunday, 7 = Saturday
    t.trade_date is not null                       as is_trading_day  -- weekday holidays are false too
from calendar as c
left join trading_days as t
    on c.date_day = t.trade_date

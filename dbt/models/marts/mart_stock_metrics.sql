-- Per stock per day: daily return, 20/50-day moving averages, 30-day volatility.
-- All windows count TRADING days (ROWS), not calendar days, because the fact has no
-- rows on weekends/holidays.

with prices as (

    select ticker, trade_date, price_id, close_price, adj_close_price, volume
    from {{ ref('fact_daily_prices') }}

),

returns as (

    select
        *,
        -- Adjusted close, so a stock split doesn't look like a crash.
        -- SAFE_DIVIDE returns NULL instead of erroring if the previous price is 0/NULL.
        safe_divide(adj_close_price, lag(adj_close_price) over by_ticker) - 1 as daily_return
    from prices
    window by_ticker as (partition by ticker order by trade_date)

),

rolling as (

    select
        *,
        avg(adj_close_price) over last_20 as ma_20_raw,
        count(*)             over last_20 as days_in_20,
        avg(adj_close_price) over last_50 as ma_50_raw,
        count(*)             over last_50 as days_in_50,
        stddev_samp(daily_return) over last_30 as stddev_30,
        count(daily_return)       over last_30 as returns_in_30
    from returns
    window
        last_20 as (partition by ticker order by trade_date rows between 19 preceding and current row),
        last_50 as (partition by ticker order by trade_date rows between 49 preceding and current row),
        last_30 as (partition by ticker order by trade_date rows between 29 preceding and current row)

)

select
    price_id,
    ticker,
    trade_date,
    close_price,
    adj_close_price,
    volume,
    daily_return,
    -- Only publish a moving average once the full window exists; a "20-day" average
    -- of 3 days would be misleading on the dashboard.
    if(days_in_20 = 20, ma_20_raw, null)               as ma_20,
    if(days_in_50 = 50, ma_50_raw, null)               as ma_50,
    -- Annualised: daily standard deviation x sqrt(252 trading days a year).
    if(returns_in_30 = 30, stddev_30 * sqrt(252), null) as volatility_30d_annualized
from rolling

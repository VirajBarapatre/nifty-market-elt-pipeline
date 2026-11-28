-- Per stock per day: daily return, 20/50-day moving averages, 30-day volatility.
-- All windows count TRADING days (ROWS), not calendar days, because the fact has no
-- rows on weekends/holidays.
-- On a known corporate-action day (seed known_corporate_actions) daily_return is NULL:
-- the price jump is a demerger or a bad split adjustment, not a real move.

with prices as (

    select ticker, trade_date, price_id, close_price, adj_close_price, volume
    from {{ ref('fact_daily_prices') }}

),

corporate_actions as (

    select ticker, action_date as trade_date, action_type
    from {{ ref('known_corporate_actions') }}

),

returns as (

    select
        p.*,
        ca.action_type is not null as is_corporate_action_day,
        -- Adjusted close, so a stock split doesn't look like a crash.
        -- SAFE_DIVIDE returns NULL instead of erroring if the previous price is 0/NULL.
        if(ca.action_type is null,
           safe_divide(p.adj_close_price, lag(p.adj_close_price) over by_ticker) - 1,
           null) as daily_return
    from prices as p
    left join corporate_actions as ca
        using (ticker, trade_date)
    window by_ticker as (partition by p.ticker order by p.trade_date)

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
    is_corporate_action_day,
    -- Only publish a moving average once the full window exists; a "20-day" average
    -- of 3 days would be misleading on the dashboard.
    if(days_in_20 = 20, ma_20_raw, null)               as ma_20,
    if(days_in_50 = 50, ma_50_raw, null)               as ma_50,
    -- Annualised: daily standard deviation x sqrt(252 trading days a year).
    if(returns_in_30 = 30, stddev_30 * sqrt(252), null) as volatility_30d_annualized
from rolling

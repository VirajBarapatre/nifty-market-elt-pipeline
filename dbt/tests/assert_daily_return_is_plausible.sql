-- Singular test: no Nifty 50 stock should move more than 25% in one day.
-- A hit usually means a split that wasn't adjusted, or a bad price from yfinance.
-- Severity warn: it's a signal to investigate, not proof the pipeline is broken.
{{ config(severity='warn') }}

select ticker, trade_date, daily_return
from {{ ref('mart_stock_metrics') }}
where abs(daily_return) > 0.25

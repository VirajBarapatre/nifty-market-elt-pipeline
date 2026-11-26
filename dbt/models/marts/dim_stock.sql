-- One row per Nifty 50 stock: who it is and which sector it belongs to.

select
    ticker,
    symbol,
    company_name,
    sector,
    isin
from {{ ref('stg_stocks') }}

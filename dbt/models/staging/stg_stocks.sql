-- Staging for the official NSE Nifty 50 list (seed: ind_nifty50list.csv).
-- Column names in NSE's file contain spaces, so they need backticks.

with source as (

    select * from {{ ref('ind_nifty50list') }}

)

select
    trim(`Symbol`)                    as symbol,
    concat(trim(`Symbol`), '.NS')     as ticker,        -- matches the yfinance ticker in prices
    trim(`Company Name`)              as company_name,
    trim(`Industry`)                  as sector,        -- NSE calls it "Industry"
    trim(`Series`)                    as series,
    trim(`ISIN Code`)                 as isin
from source

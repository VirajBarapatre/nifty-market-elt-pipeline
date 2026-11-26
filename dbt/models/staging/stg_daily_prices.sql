-- Staging for daily prices: rename, cast, drop bad rows, dedupe.
-- One row per ticker per trading day. No business logic here (that's marts).

with source as (

    select * from {{ source('raw', 'daily_prices') }}

),

renamed as (

    select
        -- Surrogate key: one value per ticker+date, so a plain `unique` test
        -- can check the grain without an extra dbt package.
        concat(ticker, '_', cast(trade_date as string))  as price_id,
        ticker,
        regexp_replace(ticker, r'\.NS$', '')              as symbol,   -- RELIANCE.NS -> RELIANCE, to join the NSE list
        cast(trade_date as date)                          as trade_date,
        cast(open as float64)                             as open_price,
        cast(high as float64)                             as high_price,
        cast(low as float64)                              as low_price,
        cast(close as float64)                            as close_price,
        cast(adj_close as float64)                        as adj_close_price,
        cast(volume as int64)                             as volume,
        ingested_at
    from source

),

cleaned as (

    -- Python already rejects these, but staging must not trust upstream:
    -- someone could load the raw table another way.
    select *
    from renamed
    where ticker is not null
      and trade_date is not null
      and close_price > 0
      and adj_close_price > 0
      and high_price >= low_price
      and volume > 0

),

deduped as (

    -- If the same ticker+date ever appears twice, keep the most recently ingested row.
    select *
    from cleaned
    qualify row_number() over (
        partition by ticker, trade_date
        order by ingested_at desc
    ) = 1

)

select * from deduped

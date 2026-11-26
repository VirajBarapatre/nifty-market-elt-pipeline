-- The 5 biggest gainers and 5 biggest losers on each trading day.

with daily as (

    select
        m.trade_date,
        m.ticker,
        s.company_name,
        s.sector,
        m.daily_return
    from {{ ref('mart_stock_metrics') }} as m
    join {{ ref('dim_stock') }} as s
        using (ticker)
    where m.daily_return is not null   -- first day of history has no previous close

),

ranked as (

    select
        *,
        rank() over (partition by trade_date order by daily_return desc) as gain_rank,
        rank() over (partition by trade_date order by daily_return asc)  as loss_rank
    from daily

)

select
    concat(ticker, '_', cast(trade_date as string)) as price_id,
    trade_date,
    ticker,
    company_name,
    sector,
    daily_return,
    if(gain_rank <= 5, 'gainer', 'loser')      as mover_type,
    if(gain_rank <= 5, gain_rank, loss_rank)   as mover_rank   -- RANK(): ties share a rank
from ranked
where gain_rank <= 5 or loss_rank <= 5

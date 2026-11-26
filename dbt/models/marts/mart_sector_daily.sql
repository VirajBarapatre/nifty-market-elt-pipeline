-- Sector performance per trading day: equal-weighted average return of the
-- sector's stocks, plus how many went up vs down.
-- NOTE: equal-weighted, not market-cap weighted like the real Nifty sector indices.

with returns as (

    select
        m.trade_date,
        s.sector,
        m.daily_return
    from {{ ref('mart_stock_metrics') }} as m
    join {{ ref('dim_stock') }} as s
        using (ticker)
    where m.daily_return is not null

)

select
    concat(sector, '_', cast(trade_date as string)) as sector_day_id,
    trade_date,
    sector,
    count(*)                                  as stocks,
    avg(daily_return)                         as avg_daily_return,
    countif(daily_return > 0)                 as advancers,
    countif(daily_return < 0)                 as decliners,
    max(daily_return)                         as best_return,
    min(daily_return)                         as worst_return
from returns
group by trade_date, sector

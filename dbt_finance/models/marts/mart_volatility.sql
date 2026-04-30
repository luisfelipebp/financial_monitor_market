with daily_metrics as (
    select 
        ticker, 
        ROUND((close - open) / open * 100, 2) as pct_change
    from {{ ref('stg_stock_prices') }}
    where open is not null and close is not null
)

select 
    ticker,
    ROUND(STDDEV(pct_change), 4) as volatility_index
from daily_metrics
group by ticker
having count(pct_change) > 1 
   and STDDEV(pct_change) is not null
order by volatility_index desc
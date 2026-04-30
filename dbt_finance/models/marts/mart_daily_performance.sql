with stock_prices as (
    select 
    ticker, datetime, open, close,
        ROUND((close - open) / open * 100, 2) as pct_change,
        CASE 
            WHEN ROUND((close - open) / open * 100, 2) > 0 THEN 'Alta'
            WHEN ROUND((close - open) / open * 100, 2) < 0 THEN 'Queda'
            WHEN ROUND((close - open) / open * 100, 2) = 0 THEN 'Estável'
        END AS day_type,
        ROUND(close / LAG(close) 
        OVER (
            PARTITION BY ticker
            ORDER BY datetime)-1, 4) * 100
            AS var_vs_yesterday,
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY datetime
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW 
        ) as ma_7d,
        AVG(close) OVER (
            PARTITION BY ticker
             ORDER BY datetime 
             ROWS BETWEEN 29 PRECEDING AND CURRENT ROW 
        ) as ma_30d
     from {{ ref('stg_stock_prices') }}
)


select * from stock_prices
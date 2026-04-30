with stock_prices as (
    select 
    *
     from {{ ref('stg_stock_prices') }}
)

select * from stock_prices where close <= 0
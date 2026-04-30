with stock_prices as (
    select
        ticker as asset_id,
        datetime,
        close as price_usd,
        'Stock' as asset_type
    from {{ ref('stg_stock_prices') }}
),

crypto_prices as (
    select
        coin_id as asset_id,
        datetime,
        price_usd,
        'Crypto' as asset_type
    from {{ ref('stg_crypto_prices') }}
)

select *
from stock_prices

union all

select *
from crypto_prices
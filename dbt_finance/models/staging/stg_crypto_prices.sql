with source_crypto_quotes  as (
    SELECT 
        coin_id::varchar,
        datetime::timestamptz,
        price_usd::numeric,
        market_cap_usd::numeric,
        vol_24h_usd::numeric
         
    from {{ source('raw_finance', 'crypto_quotes') }}
)


SELECT * FROM source_crypto_quotes 

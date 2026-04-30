with source_stock_quotes as (
    SELECT 
        datetime::timestamptz,
        open::numeric,
        high::numeric,
        low::numeric,
        close::numeric,
        volume::bigint,
        dividends::numeric,
        stock_splits::numeric,
        ticker::varchar,
        capital_gains::numeric
         
    from {{ source('raw_finance', 'stock_quotes') }}
)


SELECT * FROM source_stock_quotes




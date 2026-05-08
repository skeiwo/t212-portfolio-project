{{ config(materialized='table') }}

with prices as (
    select * from {{ ref('int_historical_prices_dedup') }}
)

,final as (
    select
        -- identifiers
        record_id,
        isin,

        -- timestamps
        record_date,

        -- prices (instrument currency)
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        extract_timestamp
    from prices
)

select * from final

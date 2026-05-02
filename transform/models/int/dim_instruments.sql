{{ config(materialized='table') }}

with distinct_tickers as (
    select distinct ticker from {{ ref('stg_orders_history') }}
)

,tradable_stocks as (
    select * from {{ ref('stg_tradable_stocks') }}
)

,final as (
select
    isin,
    ticker,
    instrument_name,
    instrument_type,
    currency_code,
    added_on,
    extract_timestamp
from tradable_stocks
where exists(select 1 from distinct_tickers where tradable_stocks.ticker = distinct_tickers.ticker)
)

select * from final
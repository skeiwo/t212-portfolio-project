{{ config(materialized='table') }}

with source_orders_history as (
    select * from {{ source('t212_stg', 'stg_orders_history')}}

)
,source_tickers as (
    select * from {{source('t212_stg', 'stg_tickers')}}
)

,final as (
SELECT
  order_id,
  (select ticker from source_tickers t where oh.ticker = t.t212_ticker) ticker,
  created_at,
  filled_at,
  currency,
  order_value,
  filled_quantity,
  fill_price,
  net_value,
  fx_rate,
  extract_timestamp
FROM source_orders_history oh
WHERE True
  AND status = 'FILLED'
  AND fill_type != 'STOCK_SPLIT'
)

select * from final
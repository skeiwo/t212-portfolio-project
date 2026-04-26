{{ config(materialized='view') }}

with source as (
    select * from {{ source('t212_seeds', 'raw_tickers') }}
)

,final as (
select
  ticker,
  t212_ticker,
  isin,
  company_name,
  currency,
  market,
from source
)

select * from final
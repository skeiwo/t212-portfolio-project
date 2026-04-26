{{ config(materialized='view') }}

with source as (
    select * from {{ source('t212', 'raw_t212_tickers') }}
)

,final as (
select
  ticker,
  t212_ticker,
  isin,
  INITCAP(company_name) company_name,
  currency,
  market,
from source
)

select * from final
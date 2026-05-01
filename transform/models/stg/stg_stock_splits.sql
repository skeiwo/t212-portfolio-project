{{ config(materialized='view') }}

with source as (
    select * from {{ source('t212_raw', 'raw_stock_splits') }}
)

,final as (
select
  {{ dbt_utils.generate_surrogate_key(['ticker', 'date']) }} as surrogate_key,
  ticker,
  date,
  ratio_old,
  ratio_new
from source
)

select * from final
{{ config(materialized='table') }}

with source_open_positions as (
    select * from {{ source('t212_stg', 'stg_open_positions')}}

)
,source_tickers as (
    select * from {{source('t212_stg', 'stg_tickers')}}
)

,final as (
select
    (select ticker from source_tickers t where op.ticker = t.t212_ticker) ticker,
    op.quantity,
    op.quantity_available,
    op.quantity_in_pies,
    op.current_price,
    op.average_price_paid,
    op.total_cost,
    op.current_value,
    op.unrealized_profit_loss,
    op.fx_impact,
    op.position_created_at,
    op.extract_timestamp,
from source_open_positions op
)

select * from final
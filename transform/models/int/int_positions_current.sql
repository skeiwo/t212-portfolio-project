{{ config(materialized='table') }}

with positions as (
    select * from {{ ref('stg_open_positions') }}
)

,positions_latest as (
    select * except(snapshot_rank)
    from (
        select
            *,
            row_number() over (partition by ticker order by extract_timestamp desc) snapshot_rank
        from positions
    )
    where snapshot_rank = 1
)

,final as (
    select
        isin,
        position_created_at,
        quantity,
        quantity_available,
        quantity_in_pies,
        instrument_currency,
        current_price,
        average_price_paid,
        total_cost,
        current_value,
        unrealized_profit_loss,
        fx_impact,
        safe_divide(unrealized_profit_loss, total_cost) unrealized_profit_loss_pct,
        extract_timestamp
    from positions_latest
)

select * from final
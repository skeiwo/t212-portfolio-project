{{ config(materialized='table') }}

with positions as (
    select * from {{ ref('int_positions_current') }}
)

,final as (
    select
        -- identifiers
        isin,

        -- timestamps
        position_created_at,

        -- holdings
        quantity,
        quantity_available,
        quantity_in_pies,

        -- prices (in instrument currency)
        instrument_currency,
        current_price,
        average_price_paid,

        -- wallet impact (already EUR)
        total_cost total_cost_eur,
        current_value current_value_eur,
        unrealized_profit_loss unrealized_pnl_eur,
        unrealized_profit_loss_pct unrealized_pnl_pct,
        fx_impact fx_impact_eur,
        extract_timestamp
    from positions
)

select * from final
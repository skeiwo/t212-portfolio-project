{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('int_orders_filled') }}
)

,final as (
    select
        -- identifiers
        order_id,
        isin,

        -- timestamps
        filled_at,

        -- order metadata
        side,

        -- raw quantities and prices
        filled_quantity,
        fill_price,
        instrument_currency,

        -- monetary impact
        net_value net_value_eur,
        currency_conversion_fee fx_fee_eur,
        fx_rate,

        -- split-adjusted versions
        split_adjustment_factor,
        filled_quantity_split_adjusted,
        fill_price_split_adjusted
    from orders
)

select * from final
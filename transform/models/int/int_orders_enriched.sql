{{ config(materialized='table') }}

with orders as (
select * from {{ ref('stg_orders_history') }}
where true
    and status = 'FILLED'
    and fill_type != 'STOCK_SPLIT'
)

,orders_with_split_factor as (
select
	o.order_id,
	coalesce(sum(ratio_old / ratio_new), 1.0) as split_adjustment_factor
from orders o
left join {{ ref('stg_stock_splits') }} s
	on s.isin = o.isin
	and s.date > cast(o.filled_at as date)
group by o.order_id
),

final as (
select
	o.order_id,
	o.isin,
	o.side,
	o.filled_quantity,
	o.fill_price,
	o.instrument_currency,
	o.net_value,
	o.currency,
	o.fx_rate,
	o.currency_conversion_fee as fx_fee,
	
	adj.split_adjustment_factor,
    o.filled_quantity * adj.split_adjustment_factor as filled_quantity_split_adjusted,
    o.fill_price / adj.split_adjustment_factor as fill_price_split_adjusted,
	o.filled_at,
	o.extract_timestamp
from orders o
left join orders_with_split_factor adj on o.order_id = adj.order_id
)

select * from final
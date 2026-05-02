{{ config(materialized='table') }}

with orders as (
select * from {{ ref('stg_orders_history') }}
where true
    and status = 'FILLED'
    and fill_type != 'STOCK_SPLIT'
)

,dedup_orders as (
select
	order_id,
	isin,
	side,
	MIN(created_at) created_at,
	SUM(filled_quantity) filled_quantity,
	-- WEIGHTED AVERAGE FILL PRICE
	SUM(fill_price * filled_quantity) / SUM(filled_quantity) fill_price,
	instrument_currency,
	SUM(net_value) net_value,
	currency,
	-- WEIGHTED AVERAGE FX RATE
	SUM(fx_rate * filled_quantity) / SUM(filled_quantity) fx_rate,
	SUM(currency_conversion_fee) currency_conversion_fee,
	MAX(filled_at) filled_at,
	MAX(extract_timestamp) extract_timestamp
from orders
group by order_id, isin, side, instrument_currency, currency
)

,orders_with_split_factor as (
select
	o.order_id,
	coalesce(exp(sum(ln(s.ratio_old / s.ratio_new))), 1.0) as split_adjustment_factor
from dedup_orders o
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
	o.currency_conversion_fee,
	
	adj.split_adjustment_factor,
    o.filled_quantity * adj.split_adjustment_factor as filled_quantity_split_adjusted,
    o.fill_price / adj.split_adjustment_factor as fill_price_split_adjusted,
	o.filled_at,
	o.extract_timestamp
from dedup_orders o
left join orders_with_split_factor adj on o.order_id = adj.order_id
)

select * from final
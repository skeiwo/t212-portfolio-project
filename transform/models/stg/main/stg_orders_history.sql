{{ config(materialized='view') }}

with source as (
    select * from {{ source('t212_raw', 'raw_orders_history') }}
)

,flattened as (
select
    extract_timestamp,
    record_id order_id,
    record_created_at created_at,

    nullif(json_value(payload, '$.order.strategy'), '') strategy,
    nullif(json_value(payload, '$.order.ticker'), '') ticker,
    nullif(json_value(payload, '$.order.status'), '') status,
    nullif(json_value(payload, '$.order.currency'), '') currency,
    nullif(json_value(payload, '$.order.initiatedFrom'), '') initiated_from,
    nullif(json_value(payload, '$.order.side'), '') side,
    cast(json_value(payload, '$.order.value') as float64) order_value,
    cast(json_value(payload, '$.order.filledValue') as float64) filled_value,
    cast(json_value(payload, '$.order.extendedHours') as bool) extended_hours,

    nullif(json_value(payload, '$.order.instrument.isin'), '') isin,
    nullif(json_value(payload, '$.order.instrument.currency'), '') instrument_currency,

    cast(json_value(payload, '$.fill.quantity') as float64) filled_quantity,
    cast(json_value(payload, '$.fill.price') as float64) fill_price,
    nullif(json_value(payload, '$.fill.type'), '') fill_type,
    nullif(json_value(payload, '$.fill.tradingMethod'), '') trading_method,
    cast(json_value(payload, '$.fill.filledAt') as timestamp) filled_at,

    nullif(json_value(payload, '$.fill.walletImpact.currency'), '') wallet_currency,
    cast(json_value(payload, '$.fill.walletImpact.netValue') as float64) net_value,
    cast(json_value(payload, '$.fill.walletImpact.fxRate') as float64) fx_rate,
    cast(json_value(payload, '$.fill.walletImpact.taxes') as float64) taxes
from source
)

SELECT * from flattened
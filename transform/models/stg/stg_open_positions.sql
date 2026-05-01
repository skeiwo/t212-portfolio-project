{{ config(materialized='view') }}

with source as (
    select * from {{ source('t212_raw', 'raw_open_positions') }}
)

,final as (
select
    extract_timestamp,
    record_id ticker,

    nullif(json_value(payload, '$.instrument.name'), '') instrument_name,
    nullif(json_value(payload, '$.instrument.isin'), '') isin,
    nullif(json_value(payload, '$.instrument.currency'), '') instrument_currency,
    cast(json_value(payload, '$.createdAt') as timestamp) position_created_at,

    cast(json_value(payload, '$.quantity') as float64) quantity,
    cast(json_value(payload, '$.quantityAvailableForTrading') as float64) quantity_available,
    cast(json_value(payload, '$.quantityInPies') as float64) quantity_in_pies,

    cast(json_value(payload, '$.currentPrice') as float64) current_price,
    cast(json_value(payload, '$.averagePricePaid') as float64) average_price_paid,

    nullif(json_value(payload, '$.walletImpact.currency'), '') wallet_currency,
    cast(json_value(payload, '$.walletImpact.totalCost') as float64) total_cost,
    cast(json_value(payload, '$.walletImpact.currentValue') as float64) current_value,
    cast(json_value(payload, '$.walletImpact.unrealizedProfitLoss') as float64) unrealized_profit_loss,
    cast(json_value(payload, '$.walletImpact.fxImpact') as float64) fx_impact
from source
)

SELECT * from final
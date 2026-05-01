{{ config(materialized='view') }}

with source as (
    select * from {{ source('t212_raw', 'raw_exchange_rates') }}
),

unnested as (
select base, record_id, extract_timestamp, 'CHF' as currency, cast(json_value(payload, '$.CHF') as FLOAT64) as rate from source union all
select base, record_id, extract_timestamp, 'CZK', cast(json_value(payload, '$.CZK') as FLOAT64) from source union all
select base, record_id, extract_timestamp, 'GBP', cast(json_value(payload, '$.GBP') as FLOAT64) from source union all
select base, record_id, extract_timestamp, 'USD', cast(json_value(payload, '$.USD') as FLOAT64) from source
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id', 'currency']) }} as surrogate_key,
        record_id as date,
        currency,
        base,
        rate,
        extract_timestamp
    from unnested
)

select * from final
{{ config(materialized='view') }}

with source as (
    select * from {{ source('t212_raw', 'raw_exchange_rates') }}
),

unnested as (
    select
        record_id,
        base,
        extract_timestamp,
        currency,
        rate
    from source,
    unnest([
        struct('USD' as currency, payload.USD as rate),
        struct('GBP' as currency, payload.GBP as rate),
        struct('CZK' as currency, payload.CZK as rate),
        struct('CHF' as currency, payload.CHF as rate)
    ])
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id', 'currency']) }} as surrogate_key,
        record_id as date,
        currency,
        base,
        cast(rate as float64) as rate,
        extract_timestamp
    from unnested
)

select * from final
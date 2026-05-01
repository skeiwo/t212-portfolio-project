{{ config(materialized='view') }}

with source as (
    select * from {{ source('t212_raw', 'raw_dividends') }}
),

flattened as (
    select
        extract_timestamp,
        record_id reference,

        nullif(json_value(payload, '$.ticker'), '') ticker,
        nullif(json_value(payload, '$.instrument.name'), '') instrument_name,
        nullif(json_value(payload, '$.instrument.isin'), '') isin,
        nullif(json_value(payload, '$.instrument.currency'), '') instrument_currency,

        cast(json_value(payload, '$.quantity') as float64) quantity,
        cast(json_value(payload, '$.amount') as float64) amount,
        nullif(json_value(payload, '$.currency'), '') currency,
        cast(json_value(payload, '$.grossAmountPerShare') as float64) gross_amount_per_share,
        cast(json_value(payload, '$.amountInEuro') as float64) amount_in_euro,
        cast(json_value(payload, '$.paidOn') as timestamp) paid_on,

        nullif(json_value(payload, '$.type'), '') dividend_type
    from source
)

select * from flattened
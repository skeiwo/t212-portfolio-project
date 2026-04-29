{{ config(materialized='view') }}

with source as (
    select * from {{ source('t212_raw', 'raw_tradable_stocks') }}
),

flattened as (
    select
        extract_timestamp,
        isin,
        created_at added_on,

        nullif(json_value(payload, '$.ticker'), '') ticker,
        nullif(json_value(payload, '$.shortName'), '') short_name,
        nullif(json_value(payload, '$.name'), '') instrument_name,
        nullif(json_value(payload, '$.type'), '') instrument_type,
        nullif(json_value(payload, '$.currencyCode'), '') currency_code,

        cast(json_value(payload, '$.workingScheduleId') as int64) working_schedule_id,
        cast(json_value(payload, '$.maxOpenQuantity') as float64) max_open_quantity,
        cast(json_value(payload, '$.extendedHours') as bool) extended_hours
    from source
)

select * from flattened
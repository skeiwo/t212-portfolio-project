{{ config(materialized='view') }}

with source as (
    select * from {{ source('t212_raw', 'raw_historical_prices') }}
),

flattened as (
    select
        extract_timestamp,
        record_id,
        isin,
        ticker,
        record_date date,

        cast(json_value(payload, '$.open') as float64) open,
        cast(json_value(payload, '$.high') as float64) high,
        cast(json_value(payload, '$.low') as float64) low,
        cast(json_value(payload, '$.close') as float64) close,
        cast(json_value(payload, '$.adj_close') as float64) adj_close,
        cast(json_value(payload, '$.volume') as int64) volume
    from source
)

select * from flattened
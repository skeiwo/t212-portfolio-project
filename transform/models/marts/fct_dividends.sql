{{ config(materialized='table') }}

with dividends as (
    select * from {{ ref('int_dividends_dedup') }}
)

,final as (
    select
        -- identifiers
        reference dividend_id,
        isin,

        -- timestamps
        paid_on,

        -- amounts
        quantity shares_held_at_payment,
        gross_amount_per_share,
        instrument_currency,
        amount amount_instrument_ccy,
        amount_in_euro amount_eur,
		extract_timestamp
    from dividends
)

select * from final
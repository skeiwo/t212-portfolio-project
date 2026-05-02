{{ config(materialized='table') }}

with dividends as (
    select * from {{ ref('stg_dividends') }}
)

,dividends_deduped as (
select * except(extract_rank)
from (
	select
		*,
		row_number() over (partition by reference order by extract_timestamp desc) extract_rank
	from dividends
)
where extract_rank = 1
)

,final as (
select
	reference,
	isin,
	instrument_currency,
	quantity,
	amount,
	currency,
	gross_amount_per_share,
	amount_in_euro,
	paid_on,
	extract_timestamp
from dividends_deduped
)

select * from final
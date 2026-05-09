{{ config(materialized='table') }}

with prices as (
    select * from {{ ref('stg_historical_prices') }}
)

,prices_deduped as (
select * except(extract_rank)
from (
	select
		*,
		row_number() over (partition by record_id order by extract_timestamp desc) extract_rank
	from prices
)
where extract_rank = 1
)

,final as (
select
	record_id,
	isin,
	date,
	open,
	high,
	low,
	close,
	adj_close,
	volume,
	extract_timestamp
from prices_deduped
)

select * from final

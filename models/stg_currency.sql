drop table if exists {{target_table}};

create table {{target_table}} as (
	with cte_currency as
		(select
			CAST(index as DATE) as individual_date,
			CONCAT(TO_CHAR(CAST(index as DATE), 'MONTH'), ' - ', DATE_PART('year', CAST(index as DATE))) as year_month,
			base,
			cast(rates ->> 'USD' as numeric) as usd,  
			cast(rates ->> 'SGD' as numeric) as sgd, 
			cast(rates ->> 'JPY' as numeric) as jpy, 
			cast(rates ->> 'EUR' as numeric) as eur, 
			cast(rates ->> 'CNY' as numeric) as cny, 
			cast(rates ->> 'INR' as numeric) as inr
		from public.raw_sql_version_aud_conversion_2022
		)
	select 	cte_currency.year_month,
	cte_currency.base,
	ROUND(avg(cte_currency.usd), 3) as usd,
	ROUND(avg(cte_currency.sgd), 3) as sgd,
	ROUND(avg(cte_currency.jpy), 3) as jpy,
	ROUND(avg(cte_currency.eur), 3) as eur,
	ROUND(avg(cte_currency.cny), 3) as cny,
	ROUND(avg(cte_currency.inr), 3) as inr
	from cte_currency
	group by cte_currency.year_month, cte_currency.base
)   
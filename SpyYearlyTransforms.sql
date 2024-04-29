create table spy_yearly as 
	select
		time_bucket(to_years(1), dt) as dt,
		first(open) as open,
		max(high) as high,
		min(low) as low,
		last(close) as close,
	from main.spy_1d_adj d
	group by time_bucket(to_years(1), dt)
	order by time_bucket(to_years(1), dt);

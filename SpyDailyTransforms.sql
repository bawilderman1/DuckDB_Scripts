create table spy_1d_agg_stats as 
	select
		row_number() over run_win as dt_of_yr, 
		dt, 
		strftime(dt, '%B') as mth_nm, 
		strftime(dt, '%Y') as yr_nm, 
		round(ln(close / coalesce(lag(close)over(order by dt), open)), 4) as chg,
		min(close) over run_win as yr_lc,
		max(close) over run_win as yr_hc,
		round(ln(close / yr_hc), 4) as draw_dn,
		round(ln(close / yr_lc), 4) as run_up,
		round(ln(last(close) over mnth_win / first(open) over mnth_win), 3) as mnth_pct,
		round(ln(last(close) over agg_win / first(open) over agg_win), 3) as yr_pct,
		case when yr_pct > 0 then 1 else -1 end as yr_dir,
		max(high) over agg_win as yr_h, 
		case when high = yr_h then 1 else 0 end as h_row,
		min(low) over agg_win as yr_l, 
		case when low = yr_l then 1 else 0 end as l_row,
	from main.spy_1d_adj d
	window 
		run_win as (
			partition by time_bucket(to_years(1), dt) order by dt), 
		agg_win as (
			partition by time_bucket(to_years(1), dt)),
		mnth_win as (
			partition by time_bucket(to_months(1), dt))
	order by dt;

create table spy_monthly as 
	select
		time_bucket(to_months(1), dt) as dt,
		first(open) as open,
		max(high) as high,
		min(low) as low,
		last(close) as close,
	from main.spy_1d_adj d
	where dt >= '1993-02-01'
	group by time_bucket(to_months(1), dt)
	order by time_bucket(to_months(1), dt);

-- Monthly Highs & Lows
create table spy_monthly_highlow as 
	with data_cte as (
		select dt, strftime(dt, '%B') as mth_nm,
			round(ln(last(close) over agg_win / first(open) over agg_win), 3) as yr_pct,
			case when yr_pct > 0 then 1 else -1 end as yr_dir,
			high, max(high) over agg_win as yr_h, case when high = yr_h then 1 else 0 end as h_row,
			low, min(low) over agg_win as yr_l, case when low = yr_l then 1 else 0 end as l_row,
		from main.spy_monthly d
		window 
			agg_win as (partition by time_bucket(to_years(1), dt))
		order by dt
	)
	select dt, mth_nm, yr_pct, yr_dir, 
		yr_h as yr_high, h_row as high_row, 
		yr_l AS yr_low, l_row as low_row,
	from data_cte
	order by dt;

-- consecutive monthly bars closing in the same direction
create table spy_monthly_consec_dir as 
	with spy_monthly_cte as (
		select
			row_number() over() as bar_num,
			dt, open, high, low, close,
			case when close > open then 1 else -1 end as bar_dir,
			case when bar_dir = lag(bar_dir) over(order by dt) then 0 else 1 end as chg_dir,
		from main.spy_monthly 
		where dt >= '1993-02-01'
		order by dt
	), dir_grp_cte as (
		select *, sum(chg_dir) over(order by dt) as dir_grp
		from spy_monthly_cte
		order by dt
	), consec_dir_cte as (
		select *, row_number() over(partition by dir_grp order by dt) as consec_dir,
			lead(bar_dir) over(order by dt) as next_bar_dir
		from dir_grp_cte
		order by dt
	)
	select dt, bar_dir, dir_grp, consec_dir, next_bar_dir
	from consec_dir_cte
	order by dt;

-- monthly decycler slope
create table spy_monthly_decycler125 as 
	with RECURSIVE spy_monthly_cte as (
		select
			row_number() over(ORDER BY dt) as bar_num,
			dt, open, high, low, close,
		from main.spy_monthly 
		where dt >= '1993-02-01'
		order by dt
	), decycler_cte as (
		select bar_num, dt, open, high, low, close
			,['NaN'::Double, close] as prices
			,125 as roof_length
			,(cos(sqrt(2) * pi() / roof_length) + sin(sqrt(2) * pi() / roof_length) - 1) / cos(sqrt(2) * pi() / roof_length) as alpha
			,['NaN'::Double, 0::Double] as highpass
		from spy_monthly_cte
		where bar_num = 1
		UNION ALL
		select s1.bar_num, s1.dt, s1.open, s1.high, s1.low, s1.close
			,[c.prices[-1], s1.close] as prices
			,c.roof_length
			,c.alpha
			,case when s1.bar_num <= 2 then [0::Double, 0::Double]
			 else [c.highpass[-1], pow(1 - c.alpha / 2, 2) * (s1.close - 2 * c.prices[-1] + c.prices[-2]) + 2 * (1 - c.alpha) * c.highpass[-1] - pow(1 - c.alpha, 2) * c.highpass[-2]]
			 end as highpass 
		from decycler_cte c
		join spy_monthly_cte s1 on c.bar_num = s1.bar_num - 1
	)
	select
		dt,
		case when bar_num <= 2 then 'NaN'::Double else round(close - highpass[-1], 2) end as decycler,
		round(ln(decycler / (lag(decycler)OVER(order by bar_num))), 3) as decycler_slope,
	from decycler_cte
	order by bar_num;
	
FROM spy_monthly_decycler125
ORDER BY dt;

-- monthly geometric avg slope
create table spy_monthly_geomavg5 as 
	with spy_monthly_cte as (
		select
			row_number() over(ORDER BY dt) as bar_num,
			dt, open, high, low, close,
		from main.spy_monthly 
		where dt >= '1993-02-01'
		order by dt
	), geom_cte as (
		SELECT 
			bar_num, dt, open, high, low, close,
			round((close + open) / 2, 2) as oc2,
			CASE WHEN bar_num <= 5 THEN 'NaN'::Double ELSE avg(ln(oc2)) OVER geom_length END as avg_nlog,
			round(exp(avg_nlog), 2) as geom_avg,
		FROM spy_monthly_cte
		WINDOW geom_length as (
			ORDER BY dt 
			ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
		ORDER BY dt
	)
	select dt, geom_avg,
		round(ln(geom_avg / lag(geom_avg)over(order by dt)), 3) as slope,
	from geom_cte
	order by dt;

-- monthly augen spikes
create table spy_monthly_augenspikes as
	with spy_monthly_cte as (
		select
			row_number() over(ORDER BY dt) as bar_num,
			dt, open, high, low, close,
			lag(close) over(ORDER BY dt) AS prev_close,
		from main.spy_monthly 
		where dt >= '1993-02-01'
		order by dt
	), spike_cte as (
		SELECT 
			bar_num, dt, open, high, low, close,
			round(ln(close / prev_close), 6) as log_c2c_chg,
			round(case 
				when bar_num <= 20 then 'NaN'::Double 
				else stddev_pop(log_c2c_chg)OVER(ORDER BY dt ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) 
			 end, 6) as c2c_std_dev,
			round(log_c2c_chg / c2c_std_dev, 2) as c2c_augen_spike,
			round(ln(close / open), 6) as log_o2c_chg,
			round(case 
				when bar_num <= 20 then 'NaN'::Double 
				else stddev_pop(log_o2c_chg)OVER(ORDER BY dt ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) 
			 end, 6) as o2c_std_dev,
			round(log_o2c_chg / o2c_std_dev, 2) as o2c_augen_spike,
		FROM spy_monthly_cte
		ORDER BY dt
	)
	SELECT dt, c2c_std_dev, c2c_augen_spike, o2c_std_dev, o2c_augen_spike, 
	FROM spike_cte
	ORDER BY dt;

-- monthly factor calcs
create table spy_monthly_factorcalcs as
	with spy_monthly_cte as (
		select
			row_number() over(ORDER BY dt) as bar_num,
			dt, open, high, low, close,
			lag(close) over(ORDER BY dt) AS prev_close,
		from main.spy_monthly 
		where dt >= '1993-02-01'
		order by dt
	), factors_cte as (
		select dt,
			round((close + open) / 2, 2) as oc2,
			lag(close)over(order by dt) as prev_close, lag(low)over(order by dt) as prev_low,
			high - low as rng, lag(rng)over(order by dt) as prev_rng, 
			round((open - low) / prev_rng, 2) as open_prev_rng,
			round((close - low) / rng, 3) as close_rng,
			round(abs(open - close)/rng, 2) as body2rng,
			case when body2rng <= 0.15 and close_rng > 0.66 then 'hammer'
				when body2rng <= 0.15 and close_rng between 0.33 and 0.66 then 'doji'
				when body2rng <= 0.15 and close_rng < 0.33 then 'inv_hammer'
				when body2rng > 0.8 then 'marubozu'
				else 'other' 
			end as bar_type,
			round(ln(open/prev_close), 3) as pc2o_pct,
			round(ln(close/open), 3) as o2c_pct,
			round(ln(close/prev_close), 3) as c2c_pct,
			round(ln(close / first(open) over run_win), 3) as ytd_pct,
			round(ln(last(close) over run_win / close), 3) as dtye_pct,
		from spy_monthly_cte
		window 
			run_win as (partition by time_bucket(to_years(1), dt))
		order by dt
	)
	select dt, oc2, 
		rng, open_prev_rng, close_rng, body2rng, bar_type, 
		pc2o_pct, o2c_pct, c2c_pct, ytd_pct, dtye_pct,
	from factors_cte
	order by dt;

/*
SELECT m.*, 
	hl.* EXCLUDE (dt),
	cd.* EXCLUDE (dt),
	d.* EXCLUDE (dt),
	ga.* EXCLUDE (dt),
	sp.* EXCLUDE (dt),
	fc.* EXCLUDE (dt),
FROM spy_monthly m
	LEFT JOIN spy_monthly_highlow hl ON m.dt = hl.dt
	LEFT JOIN spy_monthly_consec_dir cd ON m.dt = cd.dt
	LEFT JOIN spy_monthly_decycler125 d ON m.dt = d.dt
	LEFT JOIN spy_monthly_geomavg5 ga ON m.dt = ga.dt
	LEFT JOIN spy_monthly_augenspikes sp ON m.dt = sp.dt
	LEFT JOIN spy_monthly_factorcalcs fc ON m.dt = fc.dt
ORDER BY m.dt;
*/
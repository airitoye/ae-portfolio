/*
-- Title: Daily Climate Time Series transformed to SCD (dml)
-- Purpose: Build a scd table to optimize performance of a daily climate time series dataset for downstream BI tools
-- Data link: https://www.kaggle.com/datasets/sumanthvrao/daily-climate-time-series-data?resource=download
-- Author: Airi Oye
-- Creation Date: 9/17/25
*/

create or replace temp table tmpAvg as (
    select 
        date
        , avg(meantemp) avg_tmp
        , sum(humidity) avg_hmd
        , sum(wind_speed) avg_spd
        , sum(meanpressure) avg_prs
    from 
        data.csv
    group by all
);

create or replace temp table tmpBounds as (
    select
        month(date) date_month
    	, to_double(percentile_cont(0.025) within group (order by avg_tmp)) as min_tmp
    	, to_double(percentile_cont(0.975) within group (order by avg_tmp)) as max_tmp 
    	, to_double(percentile_cont(0.025) within group (order by avg_hmd)) as min_hmd
    	, to_double(percentile_cont(0.975) within group (order by avg_hmd)) as max_hmd
    	, to_double(percentile_cont(0.025) within group (order by avg_spd)) as min_spd
    	, to_double(percentile_cont(0.975) within group (order by avg_spd)) as max_spd
    	, to_double(percentile_cont(0.025) within group (order by avg_prs)) as min_prs
    	, to_double(percentile_cont(0.975) within group (order by avg_prs)) as max_prs
    from 
        tmpAvg
    group by all
);

create or replace temp table tmpLabel as (
    select 
        date
        , avg_tmp
        , avg_hmd
        , avg_spd
        , avg_prs
        , case 
            when avg_tmp between min_tmp and max_tmp then 0
            when avg_hmd between min_hmd and max_hmd then 0
            when avg_spd between min_spd and max_spd then 0
            when avg_prs between min_prs and max_prs then 0
            else 1 end as is_out_of_bounds      
        , case 
            when is_out_of_bounds = 0 then 'within bounds'
            when is_out_of_bounds = 1 then concat('outside bounds', 
                (case 
                    when avg_tmp not between min_tmp and max_tmp then ', temperature'
                    when avg_hmd not between min_hmd and max_hmd then ', humidity'
                    when avg_spd not between min_spd and max_spd then ', wind speed'
                    when avg_prs not between min_prs and max_prs then ', pressure'
                    else '' end))
            else '' end as label      
    from 
        tmpAvg a
        left join 
            tmpBounds b
            on month(a.business_date) = b.date_month
    order by 1
);

create or replace temp table tmpChange as (
    select
        date
        , avg_tmp
        , avg_hmd
        , avg_spd
        , avg_prs
        , label
        , is_out_of_bounds
        , lag(is_out_of_bounds) over (order by date) as previous_is_out_of_bounds
        , case 
            when is_out_of_bounds <> previous_is_out_of_bounds then 1 
            else 0 end as is_change
    from 
        tmpLabel
);

create or replace temp table tmpChangeNum as (
    select
        date
        , label
        , sum(is_change) over(order by date
            rows between unbounded preceding and current row) as is_change_cumulative
    from 
        tmpChange
);

create or replace temp table tmpChangeNumAgg as (
    select 
        is_change_cumulative
        , label
        , min(date) as start_date  
        , max(date) as end_date
    from 
        tmpChangeNum
    group by all
    order by is_change_cumulative
);

truncate table climate_time_series_scd;

insert into climate_time_series_scd
    select 
        start_date
        , end_date 
        , label
    from 
        tmpChangeNumAgg
    order by start_date
;
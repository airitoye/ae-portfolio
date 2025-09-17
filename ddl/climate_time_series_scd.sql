/*
-- Title: Daily Climate Time Series transformed to SCD (ddl)
-- Purpose: Build a scd table to optimize performance of a daily climate time series dataset for downstream BI tools
-- Data link: https://www.kaggle.com/datasets/sumanthvrao/daily-climate-time-series-data?resource=download
-- Author: Airi Oye
-- Creation Date: 9/17/25
*/

create or replace table climate_time_series_scd (
    start_date date
    , end_date date
    , label varchar
);
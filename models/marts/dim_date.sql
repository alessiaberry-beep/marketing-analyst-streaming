with stg as (
    select * from {{ ref('stg_meta_ads_insights') }}
),

dim as (
    select distinct
        report_date,
        year(report_date)                                   as year,
        month(report_date)                                  as month,
        day(report_date)                                    as day,
        dayofweek(report_date)                              as day_of_week,
        dayname(report_date)                                as day_name,
        monthname(report_date)                              as month_name,
        quarter(report_date)                                as quarter,
        case when dayofweek(report_date) in (1, 7)
            then true else false end                        as is_weekend,
        date_trunc('week', report_date)::date               as week_start_date,
        date_trunc('month', report_date)::date              as month_start_date
    from stg
)

select * from dim

with stg as (
    select * from {{ ref('stg_meta_ads_insights') }}
),

fct as (
    select
        -- keys
        {{ dbt_utils.generate_surrogate_key(['ad_id', 'report_date', 'publisher_platform']) }} as performance_id,
        ad_id,
        adset_id,
        campaign_id,
        report_date,

        -- dimensions
        publisher_platform,
        objective,
        optimization_goal,

        -- spend metrics
        spend,
        impressions,
        reach,
        frequency,
        clicks,
        unique_clicks,
        cpm,
        cpc,
        cpp,
        ctr,

        -- conversion funnel
        link_clicks,
        landing_page_views,
        initiate_checkouts,
        purchases,
        purchase_value,

        -- cost metrics
        cost_per_link_click,
        cost_per_landing_page_view,
        cost_per_initiate_checkout,
        cost_per_purchase,

        -- derived
        roas

    from stg
)

select * from fct

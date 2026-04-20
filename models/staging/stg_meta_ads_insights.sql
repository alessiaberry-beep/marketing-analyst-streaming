with source as (
    select * from {{ source('raw', 'raw_meta_ads_insights') }}
),

renamed as (
    select
        -- ids
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        objective,
        adset_id,
        adset_name,
        optimization_goal,
        ad_id,
        ad_name,

        -- dimensions
        publisher_platform,

        -- dates
        date_start::date as report_date,

        -- metrics (cast from raw)
        impressions::integer as impressions,
        reach::integer as reach,
        frequency::float as frequency,
        spend::float as spend,
        clicks::integer as clicks,
        unique_clicks::integer as unique_clicks,
        cpm::float as cpm,
        cpc::float as cpc,
        cpp::float as cpp,
        ctr::float as ctr,

        -- conversion metrics
        link_clicks::integer as link_clicks,
        landing_page_views::integer as landing_page_views,
        initiate_checkouts::integer as initiate_checkouts,
        purchases::integer as purchases,
        purchase_value::float as purchase_value,
        cost_per_link_click::float as cost_per_link_click,
        cost_per_landing_page_view::float as cost_per_landing_page_view,
        cost_per_initiate_checkout::float as cost_per_initiate_checkout,
        cost_per_purchase::float as cost_per_purchase,

        -- derived metrics
        case
            when purchases > 0 then round(purchase_value / spend, 4)
            else 0
        end as roas,

        -- audit
        _loaded_at

    from source
)

select * from renamed

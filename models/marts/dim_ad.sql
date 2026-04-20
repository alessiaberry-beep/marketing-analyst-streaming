with stg as (
    select * from {{ ref('stg_meta_ads_insights') }}
),

dim as (
    select distinct
        ad_id,
        ad_name,
        adset_id,
        adset_name,
        campaign_id,
        campaign_name,
        publisher_platform,
        objective,
        optimization_goal
    from stg
)

select * from dim

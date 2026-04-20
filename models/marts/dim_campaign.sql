with stg as (
    select * from {{ ref('stg_meta_ads_insights') }}
),

dim as (
    select distinct
        campaign_id,
        campaign_name,
        objective,
        adset_id,
        adset_name,
        optimization_goal,
        ad_id,
        ad_name
    from stg
)

select * from dim

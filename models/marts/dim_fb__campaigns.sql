{% set partition_range = "order by date_start desc rows between unbounded preceding and unbounded following"%}

WITH selected_fields AS (
    SELECT
        account_id,
        campaign_id,
        adset_id,
        ad_id,
        campaign_name,
        adset_name,
        ad_name,
        date_start,
        {{dbt_utils.generate_surrogate_key(["account_id","campaign_id","adset_id","ad_id"])}} as ad_key,
    FROM
        {{ ref('stg_fb__ad_insights') }}
        qualify row_number() over (partition by account_id,campaign_id,adset_id,ad_id order by date_start desc) = 1
),
current_campaign_name as (
    select distinct
    * except(campaign_name),
    first_value(campaign_name) over (partition by campaign_id {{partition_range}}) as campaign_name,
    from selected_fields
),
current_adset_name as (
    select distinct
    * except(adset_name),
    first_value(adset_name) over (partition by campaign_id, adset_id {{partition_range}}) as adset_name,
    from current_campaign_name
),
current_ad_name as (
    select distinct
    * except(ad_name),
    first_value(ad_name) over (partition by campaign_id, adset_id,ad_id {{partition_range}}) as ad_name,
    from current_adset_name
)
select a.* except(date_start), 
acc.account_name,
from current_ad_name a
left join {{ref('stg_fb__ad_accounts')}} acc on a.account_id = acc.account_id
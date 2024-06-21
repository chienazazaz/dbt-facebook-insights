{{
    config(
    materialized = 'table',
    )
}}

{%- set excluded_metrics = [ 
"page_video_views_by_paid_non_paid",
"page_video_views_by_uploaded_hosted",
"page_negative_feedback_by_type",
"page_impressions_by_locale_unique",
"page_fans_online",
"page_consumptions_by_consumption_type",
"page_impressions_by_story_type_unique",
"page_actions_post_reactions_total",
"page_negative_feedback_by_type_unique",
"page_fan_adds_by_paid_non_paid_unique",
"page_cta_clicks_logged_in_unique",
"page_consumptions_by_consumption_type_unique",
"page_impressions_viral_frequency_distribution",
"page_impressions_by_story_type",
"page_cta_clicks_logged_in_total",
"page_impressions_by_age_gender_unique",
"page_impressions_by_city_unique",
"page_impressions_by_country_unique",
"page_fans_city",
"page_fans_country",
"page_fans_locale",
    ] -%}

WITH extracted_metric_value AS (
    SELECT
        p.* EXCEPT(VALUES,last_sync_at),
        TIMESTAMP(json_value(metric_values, '$.end_time')) AS metric_time,
        safe_cast(json_value(metric_values, '$.value') AS float64) AS metric_value,
    FROM
        {{ ref('stg_fb__post_insights') }} p,
        unnest(VALUES) AS metric_values
    WHERE
        metric_name NOT IN (
            '{{ excluded_metrics | join("','") }}'
        )
) 
{{ dbt_utils.deduplicate(
    relation = 'extracted_metric_value',
    partition_by = 'metric_id,metric_time',
    order_by = 'metric_time desc'
) }}

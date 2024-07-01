{{
    config(
    materialized = 'table',
    )
}}

{%- set excluded_metrics = [ 
"post_activity_by_action_type",
"post_activity_by_action_type_unique",
"post_clicks_by_type",
"post_clicks_by_type_unique",
"post_impressions_by_paid_non_paid",
"post_impressions_by_story_type",
"post_impressions_by_story_type_unique",
"post_negative_feedback_by_type",
"post_negative_feedback_by_type_unique",
"post_reactions_by_type_total",
"post_video_retention_graph",
"post_video_retention_graph_autoplayed",
"post_video_retention_graph_clicked_to_play",
"post_video_view_time_by_age_bucket_and_gender",
"post_video_view_time_by_country_id",
"post_video_view_time_by_distribution_type",
"post_video_view_time_by_region_id",
"post_video_views_by_distribution_type",
    ] -%}

WITH extracted_metric_value AS (
    SELECT
        p.* EXCEPT(VALUES,last_sync_at),
        date_add(date(TIMESTAMP(json_value(metric_values, '$.end_time'))), interval 1 day) AS metric_time,
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

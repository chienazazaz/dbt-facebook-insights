{{
    config(
    materialized = 'table',
    )
}}

{% set metrics_by_demographic_groups = [
    "post_video_view_time_by_age_bucket_and_gender", 
]%}

WITH extracted_metric_value AS (
    SELECT
        p.* EXCEPT(VALUES,last_sync_at),
        TIMESTAMP(json_value(metric_values, '$.end_time')) AS metric_time,
        json_extract(metric_values, '$.value') AS metric_value,
    FROM
        {{ ref('stg_fb__post_insights') }} p,
        unnest(VALUES) AS metric_values
    WHERE
        metric_name IN (
            '{{ metrics_by_demographic_groups | join("','") }}'
        )
),
unnested_data as (
    select e.* except(metric_value),
case split(json_value(m,"$.key"),".")[safe_offset(0)] 
    when "F" then "Female" 
    when 'M' then 'Male' 
    when 'U' then 'Unknown' 
    end as gender_group,
split(json_value(m,"$.key"),".")[safe_offset(1)] as age_group,
json_value(m,"$.value") as metric_value,
from extracted_metric_value e, unnest({{var("json_transform_schema")}}.json_transform(metric_value)) as m
)

{{dbt_utils.deduplicate(
    relation = 'unnested_data',
    partition_by = 'metric_id,metric_time,gender_group,age_group',
    order_by = 'metric_time desc'
)}}
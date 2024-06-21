{{
    config(
    materialized = 'table',
    )
}}

{% set metrics_by_cities = [
    "page_impressions_by_city_unique", 
]%}

WITH extracted_metric_value AS (
    SELECT
        p.* EXCEPT(VALUES,last_sync_at),
        TIMESTAMP(json_value(metric_values, '$.end_time')) AS metric_time,
        json_extract(metric_values, '$.value') AS metric_value,
    FROM
        {{ ref('stg_fb__page_insights') }} p,
        unnest(VALUES) AS metric_values
    WHERE
        metric_name IN (
            '{{ metrics_by_cities | join("','") }}'
        )
),
unnested_data as (
    select e.* except(metric_value),
json_value(m,"$.key") as city,
json_value(m,"$.value") as metric_value,
from extracted_metric_value e, unnest({{var("json_transform_schema")}}.json_transform(metric_value)) as m
)

{{dbt_utils.deduplicate(
    relation = 'unnested_data',
    partition_by = 'metric_id,metric_time,city',
    order_by = 'metric_time desc'
)}}
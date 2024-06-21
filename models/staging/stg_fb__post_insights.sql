with deduplicate as (
    {{dbt_utils.deduplicate(
        relation=source('fb','post_insights'),
        partition_by = "id",
        order_by="_batched_at desc"
    )}}
)
select
    json_value(data,'$.id') as metric_id,
    split(json_value(data,'$.id'),"/")[safe_offset(0)] as post_id,
    json_value(data,'$.name') as metric_name,
    json_value(data,'$.period') as aggregate_period,
    json_value(data,'$.title') as metric_title,
    json_value(data,'$.description') as metric_description,
    json_extract_array(data,'$.values') as values,
    _batched_at as last_sync_at,
from deduplicate
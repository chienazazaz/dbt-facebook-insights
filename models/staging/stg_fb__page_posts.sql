


with deduplicate as (
    {{dbt_utils.deduplicate(
        relation=source('fb','page_posts'),
        partition_by= "json_value(data,'$.id')",
        order_by= "_batched_at desc"
    )}}
)
select 
  json_value(data,'$.id') as post_id,
  split(json_value(data,'$.id'),'_')[offset(0)] as page_id,
  json_value(data,'$.message') as body_content,
  timestamp(json_value(data,'$.created_time')) as created_time,
  json_value(data,'$.permalink_url') as permalink_url,
  from deduplicate
with source as (
    {{dbt_utils.deduplicate(relation = source('fb', 'pages'), partition_by= "json_value(data,'$.id')", order_by="_batched_at desc")}}
)

select 
    json_value(data,'$.id') as page_id,
    json_value(data,'$.name') as page_name,
    json_value(data,'$.about') as page_about,
    json_value(data,'$.category') as page_category,
    json_value(data,'$.global_brand_page_name') as global_brand_page_name,
    json_value(data,'$.has_transitioned_to_new_page_experience') as has_transitioned_to_new_page_experience,
    json_value(data,'$.link') as page_link,
    json_value(data,'$.phone') as page_phone,
    json_value(data,'$.website') as page_website,
    json_value(data,'$.store_code') as store_code,
    json_value(data,'$.store_number') as store_number,
    json_value(data,'$.single_line_address') as single_line_address,
    json_value(data,'$.business.id') as bm_id,
    json_value(data,'$.business.name') as bm_name,
from source

with source as (
    {{dbt_utils.deduplicate(relation = source('fb', 'ad_accounts'), partition_by= "json_value(data,'$.id')", order_by="_batched_at desc")}}
)

select 
    safe_cast(json_value(data,'$.account_status') as bool) as is_active,
    json_value(data,'$.name') as account_name,
    json_value(data,'$.id') as account_id,
    regexp_extract(json_value(data,'$.id'),r'_(\d+)') as id,
    json_value(data,'$.owner') as bm_id,
    json_value(data,'$.business_name') as business_name,
    json_value(data,'$.currency') as currency,
    safe_cast( json_value(data,'$.is_personal') as bool) as is_personal,
    safe_cast(json_value(data,'$.age') as float64) as age,
    safe_cast(json_value(data,'$.spend_cap') as float64) as spend_cap,
    safe_cast(json_value(data,'$.amount_spent') as float64) as amount_spent,
    json_value(data,'$.timezone_name') as timezone_name,
    timestamp(json_value(data,'$.created_time')) as created_time,
from source
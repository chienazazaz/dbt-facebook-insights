{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'date_start',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  on_schema_change = 'sync_all_columns',
) }}


{%- set default__date_fields = ["date_start","date_stop"] -%}
{%- set default__dimensions = ["account_id","account_name","campaign_id","campaign_name","adset_name","adset_id","ad_id","ad_name"] -%}
{%- set default__metrics = ["clicks","cpc","cpm","ctr","impressions","reach","spend"] -%}
{%- set default__interactive_actions = [ "page_engagement", "post_engagement", "link_click" ] -%}
{%- set default__ecommerce_actions = [ "add_to_cart", "onsite_conversion.purchase", "offsite_conversion.fb_pixel_purchase", "offline_conversion.purchase", "purchase", ] -%}

WITH 
source as (
    select * 
    from {{source('fb','ad_insights')}}
    where parse_date('%Y%m%d',_TABLE_SUFFIX) > date_sub(current_date, interval {{var("lookback_period",3)}} day)
),

deduplicate AS (
    {{ dbt_utils.deduplicate(
        relation = 'source',
        partition_by = "json_value(data,'$.account_id'),json_value(data,'$.campaign_id'),json_value(data,'$.adset_id'),json_value(data,'$.ad_id'),json_value(data,'$.date_start')",
        order_by = "_batched_at desc"
    ) }}
) 

select 
    {%- for f in default__date_fields %}
        DATE(json_value(data, '$.{{f}}')) AS {{ f }},
    {%- endfor %}

    {%- for f in default__dimensions %}
        json_value(data,'$.{{f}}') AS {{ f }},
    {%- endfor %}

    {%- for f in default__metrics %}
        CAST(json_value(DATA, '$.{{f}}') AS float64) AS {{ f }},
    {%- endfor %}

    {%- if var("is_interactive_actions_enabled",true) %}
        {%- for m in default__interactive_actions %}
            {{fb.unnest_json_key("json_extract_array(data,'$.actions')",'action_type',m,'value','no','','float64')}} ,
        {%- endfor %}
    {%-endif%}

    {%- if var("is_ecommerce_actions_enabled",true) %}
        {%- for m in default__ecommerce_actions %}
            {{fb.unnest_json_key("json_extract_array(data,'$.action_values')",'action_type',m,'value','','value','float64')}} ,
            {{fb.unnest_json_key("json_extract_array(data,'$.actions')",'action_type',m,'value','no','','float64')}} ,
        {%- endfor %}
    {%-endif%}

    {%- if var("additional_ad_ecommerce_actions",none) is not none %}
        {%- for m in var("additional_ad_ecommerce_actions") %}
            {{fb.unnest_json_key("json_extract_array(data,'$.action_values')",'action_type',m,'value','','value','float64')}} ,
            {{fb.unnest_json_key("json_extract_array(data,'$.actions')",'action_type',m,'value','no','','float64')}} ,
        {%- endfor %}
    {%-endif%}

    {%- if var("additional_ad_interact_actions",none) is not none %}
        {%- for m in var("additional_ad_interact_actions") %}
            {{fb.unnest_json_key("json_extract_array(data,'$.actions')",'action_type',m,'value','no','','float64')}} ,
        {%- endfor %}
    {%-endif%}
    
from deduplicate

-- Unnests a single key's value from an array. Use value_type = 'lower_string_value' to produce a lowercase version of the string value

{%- macro unnest_json_key(column_to_unnest, key_field, key_to_extract,value_field,prefix="",suffix="",value_type="float64", rename_column = "default") -%}
    {{ return(adapter.dispatch('unnest_json_key', 'fb')(column_to_unnest, key_field, key_to_extract,value_field,prefix,suffix,value_type, rename_column)) }}
{%- endmacro -%}

{%- macro default__unnest_json_key(column_to_unnest, key_field, key_to_extract,value_field,prefix="",suffix="",value_type="float64", rename_column = "default") -%}
    (
    select 
        safe_cast(json_value(a.{{value_field}}) as {{value_type}})
    from unnest({{column_to_unnest}}) as a
        where json_value(a.{{key_field}}) = '{{key_to_extract}}'
        ) as  
    {%- if  rename_column == "default" %}
        {{prefix+"__" if prefix else ""}}{{ key_to_extract | replace('.','__') }}{{"__"+suffix if suffix else ""}}
    {%- else %}
        {{rename_column}}
    {%- endif %}
    
{%- endmacro -%}
{% macro create_json_transform(schema) %}
    {% call statement() %}
        CREATE or replace FUNCTION {{schema}}.json_transform(input JSON) 
        returns ARRAY < json > LANGUAGE js AS 
        """ 
        return Object.entries(input).map(([k,v]) => ({key:k,value:v})); 
        """;
    {% endcall %}
{% endmacro %}

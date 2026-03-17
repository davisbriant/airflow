{% macro get_json_keys(source_table, json_column) %}
{% if execute %}

  {% set query -%}
    select distinct 
      key
    from {{ source_table }},
    lateral flatten(input => {{ json_column }})
  {% endset %}

  {% set results = run_query(query) %}
  {% set keys = results.columns[0].values() %}

  {{ return(keys) }}
  
{% endif %}

select 1 -- dummy SQL for parsing stage
{% endmacro %}


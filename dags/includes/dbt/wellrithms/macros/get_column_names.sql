{% macro get_column_names_macro(relation) %}
  {% set columns = adapter.get_columns_in_relation(relation) %}
  {% set column_names = columns | map(attribute='name') | list %}
  {{ return(column_names) }}
{% endmacro %}

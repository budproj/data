{% macro get_start_of_week() -%}
  (select current_date - extract(dow from current_date)::integer + 1)
{%- endmacro %}

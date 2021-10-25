{% macro weeks_in_range(left, right) -%}
  (select (((extract(day FROM (date_trunc('week', {{ right }}) - date_trunc('week', {{ left }}))))/7)+1)::int)
{%- endmacro %}

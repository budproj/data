{% macro days_in_range(left, right) -%}
  (select (extract(day FROM (date_trunc('day', {{ right }}) - date_trunc('day', {{ left }}))))::int)
{%- endmacro %}

{% macro row_number(partition, order, direction) -%}
  row_number() over (partition by {{ partition }} order by {{ order }} {{ direction }})
{%- endmacro %}

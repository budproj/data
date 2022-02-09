{% macro months_in_range(left, right) -%}
  (
    select
      (
        extract(year from age) * 12 +
        extract(month from age)
      )::int
      from age({{ right }}, {{ left }})
  )
{%- endmacro %}

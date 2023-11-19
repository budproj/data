{% macro calculate_progress(check_in_alias, kr_alias) -%}
  case 
    when
      {{ check_in_alias }}.value = {{ kr_alias }}.goal
    then 1
    when
        {{ kr_alias }}.initial_value = {{ kr_alias }}.goal and {{ check_in_alias }}.value = {{ kr_alias }}.goal
    then 1
    when
        {{ kr_alias }}.initial_value = {{ kr_alias }}.goal and {{ check_in_alias }}.value <> {{ kr_alias }}.goal
    then 0
    when
      {{ kr_alias }}.type = 'ASCENDING' and {{ check_in_alias }}.value >= {{ kr_alias }}.goal
    then 1
    when 
      {{ kr_alias }}.type = 'DESCENDING' and {{ check_in_alias }}.value <= {{ kr_alias }}.goal
    then 1
    else
      greatest(
        least(
          (({{ check_in_alias }}.value - {{ kr_alias }}.initial_value)) /
          coalesce(
            nullif(({{ kr_alias }}.goal - {{ kr_alias }}.initial_value), 0),
            1
          ), 1
        ), 0
      )
    end
{%- endmacro %}

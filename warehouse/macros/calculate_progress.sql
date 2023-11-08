{% macro calculate_progress(check_in_alias, kr_alias) -%}
  case 
    when  
      {{ kr_alias }}.mode = 'ASCENDING' and {{ check_in_alias }}.value >= {{ kr_alias }}.goal
    then 100
    when 
      {{ kr_alias }}.mode = 'DESCENDING' and {{ check_in_alias }}.value <= {{ kr_alias }}.goal
    then 100
    else
      greatest(
        least(
          (({{ check_in_alias }}.value - {{ kr_alias }}.initial_value)) /
          coalesce(
            nullif(({{ kr_alias }}.goal - {{ kr_alias }}.initial_value), 0),
            1
          ), 100
        ), 0
      )
    end
{%- endmacro %}

{% macro calculate_progress(check_in_alias, kr_alias, mode) -%}
  case 
    when  
      mode = 'ASCENDING' and check_in_alias >= kr_alias 
    then 100
    when 
      mode = 'DESCENDING' and check_in_alias <= kr_alias 
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

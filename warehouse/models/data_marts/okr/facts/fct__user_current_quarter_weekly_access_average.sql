with
  user_quarter_accesses as (
    select * from {{ ref('fct__user_current_quarter_accesses') }}
  ),

  final as (
    select
      user_id,
      (
        count(*) /
        (extract('week' from current_date) - extract('week' from date_trunc('quarter', current_date)))
      ) as weekly_access_average
      from user_quarter_accesses
      group by user_id
  )

select * from final
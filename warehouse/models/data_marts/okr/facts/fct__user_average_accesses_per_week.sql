with
  user_accesses_per_week as (
    select * from {{ ref('fct__user_accesses_per_week') }}
  ),

  user_average_accesses_per_week as (
    select
      user_id,
      avg(accesses)
    from user_accesses_per_week
    group by user_id
  ),

  final as (
    select * from user_average_accesses_per_week
  )

select * from final
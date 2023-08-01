with
  user_accesses as (
    select * from {{ ref('fct__user_accesses') }}
  ),

  users as (
    select * from {{ ref('dim__user') }}
  ),

  user_weeks_since_creation as (
    select
      id as user_id,
      {{ weeks_in_range('created_at', 'current_date') }} as weeks_since_creation
    from users
  ),  

  user_weeks_delta as (
    select
      user_weeks_since_creation.*,
      generate_series(1, user_weeks_since_creation.weeks_since_creation) as weeks_delta
    from user_weeks_since_creation
  ),

  user_weeks as (
    select
      user_weeks_delta.user_id,
      date_trunc('week', current_date - user_weeks_delta.weeks_delta * 7) as week
    from user_weeks_delta
  ),

  user_accesses_truncated_by_week as (
    select
      user_accesses.*,
      date_trunc('week', user_accesses.access_time) as week
    from user_accesses
  ),

  user_weeks_with_accesses_count as (
    select
      user_weeks.user_id,
      user_weeks.week,
      count(*) filter (where user_accesses_truncated_by_week.access_time is not null) as accesses
      from user_weeks
      left join user_accesses_truncated_by_week on
        user_weeks.user_id = user_accesses_truncated_by_week.user_id and
        user_weeks.week = user_accesses_truncated_by_week.week
      group by user_weeks.user_id, user_weeks.week
  ),

  final as (
    select * from user_weeks_with_accesses_count order by week desc
  )

select * from final

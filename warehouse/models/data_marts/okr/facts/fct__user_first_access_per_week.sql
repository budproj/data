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

  user_accesses_with_rank as (
    select
      *,
      row_number() over (partition by user_id, week order by access_time asc) as rank
      from user_accesses_truncated_by_week
  ),

  user_first_weekly_access as (
    select
      *
      from user_accesses_with_rank
      where rank = 1
  ),

  user_weeks_with_first_access as (
    select
      user_weeks.*,
      user_first_weekly_access.amplitude_event_id as first_access_amplitude_event_id
      from user_weeks
      left join user_first_weekly_access on
        user_weeks.user_id = user_first_weekly_access.user_id and
        user_weeks.week = user_first_weekly_access.week
  ),

  final as (
    select * from user_weeks_with_first_access order by week desc
  )

select * from final
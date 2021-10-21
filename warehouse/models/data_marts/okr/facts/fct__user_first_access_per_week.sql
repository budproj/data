with
  user_first_access_per_day as (
    select * from {{ ref('fct__user_first_access_per_day') }}
  ),

  user_first_access_truncated_by_week as (
    select
      *,
      date_trunc('week', user_first_access_per_day.day) as week
      from user_first_access_per_day
  ),

  user_first_access_per_week as (
    select
      user_id,
      week,
      (ARRAY_AGG(first_access_amplitude_event_id) FILTER (WHERE first_access_amplitude_event_id IS NOT NULL))[1] as first_access_amplitude_event_id
      from user_first_access_truncated_by_week
      group by user_id, week
  ),

  final as (
    select * from user_first_access_per_week order by week desc
  )

select * from final
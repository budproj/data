with
  amplitude_events as (
    select * from {{ ref('dim__amplitude_event') }}
  ),

  first_event as (
    select
      *
      from amplitude_events
      order by event_time asc
      limit 1
  ),

  weeks_since_first_event as (
    select
      {{ weeks_in_range('event_time', 'current_date') }} as weeks_since_first_event
    from first_event
  ),

  week_series as (
    select
      generate_series(1, weeks_since_first_event) as week_delta
    from weeks_since_first_event
  ),

  weeks as (
    select
      date_trunc('week', event_time + interval '1w week' * week_delta) as week
    from week_series
    left join first_event on 1=1
  ),

  unique_users_per_week as (
    select
      date_trunc('week', event_time) as week,
      count(distinct user_id) as unique_users
    from amplitude_events
    group by date_trunc('week', event_time)
  ),

  final as (
    select
      weeks.week as week,
      coalesce(unique_users_per_week.unique_users, 0) as amount_of_active_users
    from weeks
    left join unique_users_per_week on unique_users_per_week.week = weeks.week
  )

select * from final

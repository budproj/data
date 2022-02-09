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

  days_since_first_event as (
    select
      {{ days_in_range('event_time', 'current_date') }} as days_since_first_event
    from first_event
  ),

  day_series as (
    select
      generate_series(1, days_since_first_event) as day_delta
    from days_since_first_event
  ),

  days as (
    select
      date_trunc('day', event_time + interval '1 day' * day_delta) as day
    from day_series
    left join first_event on 1=1
  ),

  unique_users_per_day as (
    select
      date_trunc('day', event_time) as day,
      count(distinct user_id) as unique_users
    from amplitude_events
    group by date_trunc('day', event_time)
  ),

  final as (
    select
      days.day as day,
      coalesce(unique_users_per_day.unique_users, 0) as amount_of_active_users
    from days
    left join unique_users_per_day on unique_users_per_day.day = days.day
  )

select * from final

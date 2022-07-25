with
  amplitude_events as (
    select * from {{ ref('dim__amplitude_event') }}
  ),

  users as (
    select * from {{ ref('dim__user') }}
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
      generate_series(0, days_since_first_event) as day_delta
    from days_since_first_event
  ),

  days as (
    select
      date_trunc('day', event_time + interval '1 day' * day_delta) as day
    from day_series
    left join first_event on 1=1
  ),

  is_user_active as (
    select
      distinct
      date_trunc('day', event_time) as day,
      user_id
    from amplitude_events
  ),

  final as (
    select
      days.day as day,
      users.id as user_id,
      is_user_active.user_id is not null as is_active
    from
      days
      join users on (
        users.status = 'INACTIVE'
        and days.day > users.created_at
        and days.day < users.updated_at
      )
      or (
        users.status = 'ACTIVE'
        and days.day > users.created_at
      )
      left join is_user_active on is_user_active.day = days.day
      and users.id = is_user_active.user_id
  )

select
  *
from
  final
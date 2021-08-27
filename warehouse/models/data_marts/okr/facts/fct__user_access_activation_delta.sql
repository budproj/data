with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  user_first_access as (
    select * from {{ ref('fct__user_first_access') }}
  ),

  amplitude_event as (
    select * from {{ ref('dim__amplitude_event') }}
  ),

  user_and_event_data as (
    select
      users.id as user_id,
      amplitude_event.event_time as first_access_time,
      users.created_at as user_creation_time
    from users
    left join user_first_access on users.id = user_first_access.user_id
    left join amplitude_event on user_first_access.amplitude_event_id = amplitude_event.id
  ),

  final as (
    select
      user_id,
      date_part('day', first_access_time::timestamp - user_creation_time::timestamp)::smallint as delta
    from user_and_event_data
  )

select * from final
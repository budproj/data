with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  amplitude_events as (
    select * from {{ ref('dim__amplitude_event') }}
  ),

  events_in_current_month as (
    select
      *
      from amplitude_events
      where event_time > (current_date - interval '30 day')
  ),

  events_in_current_month_per_user as (
    select
      user_id,
      count(*) as number_of_events
      from events_in_current_month
      group by user_id
  ),

  final as (
    select
      id as user_id,
      number_of_events is not null as is_active
      from users
      left join events_in_current_month_per_user on user_id = id
  )

select * from final

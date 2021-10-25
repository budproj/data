with
  user_first_access_per_day as (
    select * from {{ ref('fct__user_first_access_per_day') }}
  ),

  user_access_data as (
    select
      user_id,
      count(*) as days_since_register,
      count(*) filter (where first_access_amplitude_event_id is not null) as days_with_access
      from user_first_access_per_day
      group by user_id
  ),

  user_access_average as (
    select
      user_id,
      days_since_register / coalesce(nullif(days_with_access, 0), 1) as average
    from user_access_data
  ),

  final as (
    select * from user_access_average
  )

select * from final
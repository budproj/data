with
  user_accesses as (
    select * from {{ ref('fct__user_accesses') }}
  ),

  users as (
    select * from {{ ref('dim__user') }}
  ),

  user_with_days_since_register as (
    select
      id as user_id,
      date_part('day', current_date::timestamp - created_at::timestamp)::integer as days_since_register
    from users
  ),

  user_day_accesses as (
    select
      user_id,
      date_trunc('day', access_time) as day
      from user_accesses
  ),

  user_with_unique_day_access as (
    select
      *,
      count(*) as days_with_access
      from user_day_accesses
      group by user_id, day
  ),

  user_access_average as (
    select
      user_with_days_since_register.user_id,
      user_with_days_since_register.days_since_register / coalesce(nullif(user_with_unique_day_access.days_with_access, 0), 1) as average
    from user_with_days_since_register
    left join user_with_unique_day_access on user_with_days_since_register.user_id = user_with_unique_day_access.user_id
  ),

  final as (
    select * from user_access_average
  )

select * from final
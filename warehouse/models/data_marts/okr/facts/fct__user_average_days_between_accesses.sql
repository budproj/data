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

  user_accesses_count as (
    select
      user_id,
      count(*) as days_with_access
      from user_accesses
      group by user_id
  ),

  user_access_average as (
    select
      user_with_days_since_register.user_id,
      user_with_days_since_register.days_since_register / coalesce(nullif(user_accesses_count.days_with_access, 0), 1) as average
    from user_with_days_since_register
    left join user_accesses_count on user_with_days_since_register.user_id = user_accesses_count.user_id
  ),

  final as (
    select * from user_access_average
  )

select * from final

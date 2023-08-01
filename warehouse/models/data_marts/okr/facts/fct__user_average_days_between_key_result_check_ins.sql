with
  user_key_result_check_ins as (
    select * from {{ ref('fct__user_key_result_check_ins') }}
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

  user_day_key_result_check_ins as (
    select
      user_id,
      date_trunc('day', date) as day
      from user_key_result_check_ins
  ),

  user_with_unique_day_key_result_check_ins as (
    select
      *,
      count(*) as days_with_key_result_check_ins
      from user_day_key_result_check_ins
      group by user_id, day
  ),

  user_key_result_check_ins_average as (
    select
      user_with_days_since_register.user_id,
      user_with_days_since_register.days_since_register / coalesce(nullif(user_with_unique_day_key_result_check_ins.days_with_key_result_check_ins, 0), 1) as average
    from user_with_days_since_register
    left join user_with_unique_day_key_result_check_ins on user_with_days_since_register.user_id = user_with_unique_day_key_result_check_ins.user_id
  ),

  final as (
    select * from user_key_result_check_ins_average
  )

select * from final

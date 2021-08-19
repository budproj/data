with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  key_result_latest_check_in as (
    select * from {{ ref('fct__key_result_latest_check_in') }}
  ),

  key_result_check_in as (
    select * from {{ ref('dim__key_result_check_in') }}
  ),

  user_key_result_progress as (
    select
      users.id as user_id,
      {{ calculate_progress('key_result_check_in', 'key_result') }} as progress,
      key_result_check_in.created_at as date
      from users
      left join key_result on users.id = key_result.owner_id
      left join key_result_latest_check_in on key_result.id = key_result_latest_check_in.key_result_id
      left join key_result_check_in on key_result_latest_check_in.key_result_check_in_id = key_result_check_in.id
  ),

  final as (
    select
      user_id,
      avg(progress) as progress,
      max(date) as date
      from user_key_result_progress
      group by user_id
  )

select * from final
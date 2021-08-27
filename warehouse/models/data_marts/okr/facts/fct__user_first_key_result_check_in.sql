with
  key_result_check_in as (
    select * from {{ ref('dim__key_result_check_in') }}
  ),

  user_first_key_result_check_in as (
    select
      user_id,
      min(created_at) as check_in_time
    from key_result_check_in
    group by user_id
  ),

  final as (
    select
      key_result_check_in.user_id,
      id as key_result_check_in_id
    from user_first_key_result_check_in
    left join key_result_check_in on user_first_key_result_check_in.check_in_time = key_result_check_in.created_at
  )

select * from final
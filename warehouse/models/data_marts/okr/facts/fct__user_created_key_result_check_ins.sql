with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  key_result_check_in as (
    select * from {{ ref('dim__key_result_check_in') }}
  ),

  final as (
    select
      users.id as user_id,
      key_result_check_in.id as key_result_check_in_id,
      key_result_check_in.created_at as created_at
      from key_result_check_in
      left join users on users.id = key_result_check_in.user_id
      group by users.id, key_result_check_in.id, key_result_check_in.created_at
  )

select * from final

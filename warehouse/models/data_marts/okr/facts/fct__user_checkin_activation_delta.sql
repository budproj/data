with
  users as (
    select * from {{ ref('dim__user') }}
  ),

   key_result_check_in as (
     select * from {{ ref('dim__key_result_check_in') }}
   ),

  user_first_key_result_check_in as (
    select * from {{ ref('fct__user_first_key_result_check_in') }}
  ),

  final as (
    select
      users.id as user_id,
      date_part('day', key_result_check_in.created_at::timestamp - users.created_at::timestamp)::smallint as delta
    from users
    left join user_first_key_result_check_in on users.id = user_first_key_result_check_in.user_id
    left join key_result_check_in on user_first_key_result_check_in.key_result_check_in_id = key_result_check_in.id
  )

select * from final
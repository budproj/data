with
  users as (
    select * from {{ ref('dim__user') }}
  ),

   key_result_comment as (
     select * from {{ ref('dim__key_result_comment') }}
   ),

  user_first_key_result_comment as (
    select * from {{ ref('fct__user_first_key_result_comment') }}
  ),

  final as (
    select
      users.id as user_id,
      date_part('day', key_result_comment.created_at::timestamp - users.created_at::timestamp)::smallint as delta
    from users
    left join user_first_key_result_comment on users.id = user_first_key_result_comment.user_id
    left join key_result_comment on user_first_key_result_comment.key_result_comment_id = key_result_comment.id
  )

select * from final
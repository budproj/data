with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  key_result_comment as (
    select * from {{ ref('dim__key_result_comment') }}
  ),

  final as (
    select
      users.id as user_id,
      key_result_comment.id as key_result_comment_id,
      key_result_comment.created_at as created_at
      from key_result_comment
      left join users on users.id = key_result_comment.user_id
  )

select * from final
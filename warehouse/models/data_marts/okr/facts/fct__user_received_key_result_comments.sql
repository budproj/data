with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  key_result_comment as (
    select * from {{ ref('dim__key_result_comment') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  final as (
    select
      key_result.owner_id as user_id,
      key_result_comment.user_id as author_id,
      key_result_comment.id as key_result_comment_id,
      key_result_comment.created_at as created_at
      from users
      left join key_result_comment on users.id = key_result_comment.user_id
      left join key_result on key_result_comment.key_result_id = key_result.id
  )

select * from final
with
  key_result_comment as (
    select * from {{ ref('dim__key_result_comment') }}
  ),

  user_first_key_result_comment as (
    select
      user_id,
      min(created_at) as comment_time
    from key_result_comment
    group by user_id
  ),

  final as (
    select
      key_result_comment.user_id,
      id as key_result_comment_id
    from user_first_key_result_comment
    left join key_result_comment on user_first_key_result_comment.comment_time = key_result_comment.created_at
  )

select * from final
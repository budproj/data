with
  src_okr__key_result_comment as (
    select * from {{ source('conformed', 'okr__key_result_comment') }}
  ),

  final as (
    select
      id::uuid,
      text::text,
      user_id::uuid,
      key_result_id::uuid,
      created_at::timestamp,
      updated_at::timestamp,
      type::text,
      extra::json,
      parent_id::uuid
    from src_okr__key_result_comment
  )

select * from final

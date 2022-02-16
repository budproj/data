with
  src_okr__key_result_check_mark as (
    select * from {{ source('conformed', 'okr__key_result_check_mark') }}
  ),

  final as (
    select
      id::uuid,
      state::varchar(32),
      description::text,
      user_id::uuid,
      key_result_id::uuid,
      assigned_user_id::uuid,
      created_at::timestamp,
      updated_at::timestamp
    from src_okr__key_result_check_mark
  )

select * from final

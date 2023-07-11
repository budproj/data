with
  src_okr__key_result_update as (
    select * from {{ source('conformed', 'okr__key_result_update') }}
  ),

  final as (
    select
      id::uuid,
      created_at::timestamp,
      key_result_id::uuid,
      author::jsonb,
      old_state::jsonb,
      patches::jsonb,
      new_state::jsonb
    from src_okr__key_result_update
  )

select * from final

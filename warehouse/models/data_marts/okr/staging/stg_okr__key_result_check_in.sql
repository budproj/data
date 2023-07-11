with
  src_okr__key_result_check_in as (
    select * from {{ source('conformed', 'okr__key_result_check_in') }}
  ),

  final as (
    select
      id::uuid,
      value::float,
      comment::text,
      user_id::uuid,
      parent_id::uuid,
      confidence::smallint,
      key_result_id::uuid,
      created_at::timestamp,
      previous_state::jsonb
    from src_okr__key_result_check_in
  )

select * from final

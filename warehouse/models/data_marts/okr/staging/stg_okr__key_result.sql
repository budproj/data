with
  src_okr__key_result as (
    select * from {{ source('conformed', 'okr__key_result') }}
  ),

  final as (
    select
      id::uuid,
      goal::float,
      type::varchar(32),
      title::text,
      format::varchar(32),
      team_id::uuid,
      owner_id::uuid,
      description::text,
      objective_id::uuid,
      initial_value::float,
      created_at::timestamp,
      updated_at::timestamp,
      mode::text,
      comment_count::jsonb,
      last_updated_by::jsonb
    from src_okr__key_result
  )

select * from final

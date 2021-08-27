with
  src_okr__objective as (
    select * from {{ source('conformed', 'okr__objective') }}
  ),

  final as (
    select
      id::uuid,
      title::text,
      team_id::uuid,
      cycle_id::uuid,
      owner_id::uuid,
      created_at::timestamp,
      updated_at::timestamp
    from src_okr__objective
  )

select * from final
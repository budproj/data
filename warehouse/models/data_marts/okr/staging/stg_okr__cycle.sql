with
  src_okr__cycle as (
    select * from {{ source('conformed', 'okr__cycle') }}
  ),

  final as (
    select
      id::uuid,
      active::boolean,
      period::varchar(32),
      cadence::varchar(32),
      team_id::uuid,
      date_start::timestamp,
      date_end::timestamp,
      parent_id::uuid,
      created_at::timestamp,
      updated_at::timestamp
    from src_okr__cycle
  )

select * from final

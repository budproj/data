with
  src_okr__team as (
    select * from {{ source('conformed', 'okr__team') }}
  ),

  final as (
    select
      id::uuid,
      name::varchar(256),
      gender::varchar(32),
      owner_id::uuid,
      parent_id::uuid,
      created_at::timestamp,
      updated_at::timestamp,
      description::text
    from src_okr__team
  )

select * from final
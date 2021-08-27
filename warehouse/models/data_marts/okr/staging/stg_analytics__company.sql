with
  src_analytics__company as (
    select * from {{ source('conformed', 'analytics__company') }}
  ),

  final as (
    select
      id::uuid,
      team_id::uuid,
      name::varchar(256),
      created_at::timestamp,
      updated_at::timestamp
    from src_analytics__company
  )

select * from final
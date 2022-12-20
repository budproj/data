with
  stg_okr__team as (
    select * from {{ ref('stg_okr__team') }}
  ),

  companies as (
    select * from stg_okr__team where parent_id is null
  ),

  final as  (
    select
      {{ generate_uuid() }} as id,
      id as team_id,
      name,
      created_at,
      updated_at
      from companies
  )

select * from final
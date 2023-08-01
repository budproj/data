with
  stg_okr__cycle as (
    select * from {{ ref('stg_okr__cycle') }}
  ),

  team as (
    select * from {{ ref('dim__team') }}
  ),

  final as  (
    select
      stg_okr__cycle.*,
      team.company_id as company_id
      from stg_okr__cycle
      left join team on stg_okr__cycle.team_id = team.id
  )

select * from final

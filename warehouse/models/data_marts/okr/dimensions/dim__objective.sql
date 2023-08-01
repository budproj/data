with
  stg_okr__objective as (
    select * from {{ ref('stg_okr__objective') }}
  ),

  team as (
    select * from {{ ref('dim__team') }}
  ),

  final as  (
    select
      stg_okr__objective.*,
      team.company_id as company_id
      from stg_okr__objective
      left join team on stg_okr__objective.team_id = team.id
  )

select * from final

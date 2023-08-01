with
  stg_okr__key_result as (
    select * from {{ ref('stg_okr__key_result') }}
  ),

  team as (
    select * from {{ ref('dim__team') }}
  ),

  objective as (
    select * from {{ ref('dim__objective') }}
  ),

  final as  (
    select
      stg_okr__key_result.*,
      objective.cycle_id as cycle_id,
      team.company_id as company_id
      from stg_okr__key_result
      left join team on stg_okr__key_result.team_id = team.id
      left join objective on objective.id = stg_okr__key_result.objective_id
  )

select * from final

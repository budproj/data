with
  team as (
    select * from {{ ref('dim__team') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  cycle as (
    select * from {{ ref('dim__cycle') }}
  ),

  cycle_progress as (
    select * from {{ ref('fct__cycle_progress') }}
  ),

  team_cycle_progress as (
    select
      team.id as team_id,
      cycle.id as cycle_id,
      cycle_progress.progress,
      cycle_progress.date
      from team
      left join key_result on team.id = key_result.team_id
      left join cycle on key_result.cycle_id = cycle.id
      left join cycle_progress on cycle.id = cycle_progress.cycle_id
  ),

  final as (
    select
      team_id,
      cycle_id,
      avg(progress) as progress,
      max(date) as date
      from team_cycle_progress
      group by team_id, cycle_id
  )

select * from final

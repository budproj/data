with
  team as (
    select * from {{ ref('dim__team') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  key_result_progress as (
    select * from {{ ref('fct__key_result_progress') }}
  ),

  team_key_result_progress as (
    select
      team.id as team_id,
      key_result_progress.progress,
      key_result_progress.date
      from team
      left join key_result on team.id = key_result.team_id
      left join key_result_progress on key_result.id = key_result_progress.key_result_id
  ),

  final as (
    select
      team_id,
      avg(progress) as progress,
      max(date) as date
      from team_key_result_progress
      group by team_id
  )

select * from final

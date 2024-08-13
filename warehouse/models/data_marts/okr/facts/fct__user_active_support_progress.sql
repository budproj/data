with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  key_result_support_team_members_user as (
    select * from {{ ref('stg_okr__key_result_support_team') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  cycle as (
    select * from {{ ref('dim__cycle') }}
  ),

  key_result_progress as (
    select * from {{ ref('fct__key_result_progress') }}
  ),

  user_progress_in_active_key_result as (
    select distinct
      users.id as user_id,
      key_result_progress.progress,
      key_result_progress.date
      from users
      left join key_result_support_team_members_user on users.id = key_result_support_team_members_user.user_id
      left join key_result on key_result_support_team_members_user.key_result_id = key_result.id or users.id = key_result.owner_id
      left join key_result_progress on key_result.id = key_result_progress.key_result_id
      left join cycle on key_result.cycle_id = cycle.id
      where cycle.active = true
  ),

  final as (
    select
      user_id,
      avg(progress) as progress,
      max(date) as date
      from user_progress_in_active_key_result
      group by user_id
  )

select * from final
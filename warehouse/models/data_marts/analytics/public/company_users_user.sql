with
  stg_okr__team_users_user as (
    select * from {{ ref('stg_okr__team_users_user') }}
  ),

  team as (
    select * from {{ ref('team') }}
  ),

  final as (
    select
      team.company_id,
      user_id
    from stg_okr__team_users_user
    left join team on team.id = stg_okr__team_users_user.team_id
    group by team.company_id, user_id
  )

select * from final
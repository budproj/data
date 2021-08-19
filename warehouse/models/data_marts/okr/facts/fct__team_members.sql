with
  stg_okr__team_users_user as (
    select * from {{ ref('stg_okr__team_users_user') }}
  ),

  final as (
    select team_id, user_id from stg_okr__team_users_user
  )

select * from final
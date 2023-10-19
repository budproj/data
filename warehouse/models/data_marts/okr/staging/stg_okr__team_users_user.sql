with
  src_okr__team_users_user as (
    select * from {{ source('postgres_business', 'team_users_user') }}
  ),

  final as (
    select
      team_id::uuid,
      user_id::uuid
    from src_okr__team_users_user
  )

select * from final

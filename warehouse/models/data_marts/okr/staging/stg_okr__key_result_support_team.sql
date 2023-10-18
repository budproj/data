with
  src_okr__key_result_support_team_members_user as (
    select * from {{ source('postgres_business', 'key_result_support_team_members_user') }}
  ),

  final as (
    select
      key_result_id::uuid,
      user_id::uuid
    from src_okr__key_result_support_team_members_user
  )

select * from final

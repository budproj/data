with
  src_analytics__company_users_user as (
    select * from {{ ref('company_users_user') }}
  ),

  final as (
    select
      company_id::uuid,
      user_id::uuid
    from src_analytics__company_users_user
  )

select * from final

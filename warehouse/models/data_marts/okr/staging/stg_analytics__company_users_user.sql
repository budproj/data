with
  src_analytics__company_users_user as (
    select * from {{ ref('company_users_user') }}
  ),

  final as (
    select
      company_id::text,
      user_id::uuid
    from src_analytics__company_users_user
  )

select * from final

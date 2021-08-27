with
  stg_analytics__company_users_user as (
    select * from {{ ref('stg_analytics__company_users_user') }}
  ),

  final as (
    select company_id, user_id from stg_analytics__company_users_user
  )

select * from final
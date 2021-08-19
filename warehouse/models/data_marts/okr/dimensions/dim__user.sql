with
  stg_okr__user as (
    select * from {{ ref('stg_okr__user') }}
  ),

  amplitude_event as (
    select * from {{ ref('dim__amplitude_event') }}
  ),

  seed_buddy_users_email as (
    select email from {{ ref('buddy_users') }}
  ),

  seed_sandbox_users_email as (
    select email from {{ ref('sandbox_users') }}
  ),

  users as (
    select * from stg_okr__user
  ),

  final as (
    select
      users.*,
      case
        when (
          select count(*) from seed_buddy_users_email where email = users.email
        ) = 1 then 'BUDDY'
        when (
          select count(*) from seed_sandbox_users_email where email = users.email
        ) = 1 then 'SANDBOX'
        else 'CUSTOMER'
      end as type
    from users
  )

select * from final
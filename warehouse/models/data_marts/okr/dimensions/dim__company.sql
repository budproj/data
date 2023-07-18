with
  companies as (
    select * from {{ ref('stg_analytics__company') }}
  ),

  seed_buddy_companies_name as (
    select name from {{ ref('buddy_companies') }}
  ),

  seed_sandbox_companies_name as (
    select name from {{ ref('sandbox_companies') }}
  ),

  final as  (
    select
      id,
      team_id,
      name,
      created_at :: timestamp as created_at,
      updated_at :: timestamp as updated_at,
      case
        when (
          select count(*) from seed_buddy_companies_name where name = companies.name
        ) = 1 then 'BUDDY'
        when (
          select count(*) from seed_sandbox_companies_name where name = companies.name
        ) = 1 then 'SANDBOX'
        else 'CUSTOMER'
      end as type
      from companies
  )

select * from final

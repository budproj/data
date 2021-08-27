with
  fct__company_members as (
    select * from {{ ref('fct__company_members') }}
  ),

  company_member_with_ranking as (
    select
      *,
      row_number() over (partition by user_id order by company_id desc) as rank
      from fct__company_members
  ),

  final as (
    select company_id, user_id from company_member_with_ranking where rank = 1
  )

select * from final
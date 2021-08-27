with
  fct__team_members as (
    select * from {{ ref('fct__team_members') }}
  ),

  team_member_with_ranking as (
    select
      *,
      row_number() over (partition by user_id order by team_id desc) as rank
      from fct__team_members
  ),

  final as (
    select team_id, user_id from team_member_with_ranking where rank = 1
  )

select * from final
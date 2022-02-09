with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  cycle as (
    select * from {{ ref('dim__cycle') }}
  ),

  key_result_support_team as (
    select * from {{ ref('fct__key_result_support_team') }}
  ),

  user_owned_key_result as (
    select
      owner_id as user_id,
      count(owner_id) as number_of_owned_key_results
    from key_result
    left join cycle on cycle.id = key_result.cycle_id
    where cycle.active = true
    group by owner_id
  ),

  user_support_team_member as (
    select
      user_id,
      count(user_id) as number_of_support_team_memberships
    from key_result_support_team
    left join key_result on key_result_support_team.key_result_id = key_result.id
    left join cycle on key_result.cycle_id = cycle.id
    where cycle.active = true
    group by user_id
  ),

  final as (
    select
      users.id as user_id,
      coalesce(number_of_owned_key_results > 0, false) as owns_any,
      coalesce(number_of_support_team_memberships > 0, false) as is_support_team_member_of_any
    from users
    left join user_owned_key_result on users.id = user_owned_key_result.user_id
    left join user_support_team_member on users.id = user_support_team_member.user_id
  )

select * from final

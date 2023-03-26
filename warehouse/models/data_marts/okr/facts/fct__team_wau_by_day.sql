with calendar as (
  select
    distinct day
  from
    {{ ref('fct__user_is_active_by_day') }} ac
  order by
    day
),
final as (
  select
    c.day,
    ac.user_type,
    ac.flag_owns_team,
    ftm.team_id,
    count(distinct(ac.user_id)) as unique_users,
    count(
      distinct(
        case
          when ac.is_active is true then ac.user_id
        end
      )
    ) as unique_active_users
  from
    calendar c
    join {{ ref('fct__user_is_active_by_day') }} ac on ac.day >= c.day - interval '7' day
    and ac.day <= c.day
    join {{ ref('fct__team_members') }} ftm on ac.user_id = ftm.user_id
  group by
    c.day,
    ac.user_type,
    ac.flag_owns_team,
    ftm.team_id
)
select
  *,
  unique_active_users :: float / unique_users as wau
from
  final

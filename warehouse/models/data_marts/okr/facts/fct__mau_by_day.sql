with calendar as (
  select
    distinct day
  from
    dm_okr.fct__user_is_active_by_day ac
  order by
    day
),
final as (
  select
    c.day,
    ac.user_type,
    ac.flag_owns_team,
    fcm.company_id,
    count(distinct(user_id)) as unique_users,
    count(
      distinct(
        case
          when ac.is_active is true then user_id
        end
      )
    ) as unique_active_users
  from
    calendar c
    join dm_okr.fct__user_is_active_by_day ac on ac.day >= c.day - interval '30' day
    and ac.day <= c.day
    join dm_okr.fct__company_members fcm on ac.user_id = fcm.user_id
  group by
    c.day,
    ac.user_type,
    ac.flag_owns_team,
    fcm.company_id
)
select
  *,
  unique_active_users :: float / unique_users as mau
from
  final
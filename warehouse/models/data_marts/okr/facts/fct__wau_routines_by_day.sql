with calendar as (
  select
    distinct day
  from
    {{ ref('fct__user_is_active_by_day') }} ac
  where
    day > current_date - interval '365' day
  order by
    day
),
temp_table as (
  select
    c.day,
    ac.user_id,
    row_number() over (partition by c.day, ac.user_id) as rn,
    max(
      case
        when fill_routine then 1
        else 0
      end
    ) over (partition by c.day, ac.user_id) as active
  from
    calendar c
    left join {{ ref('fct__user_is_active_by_day') }} ac on ac.day >= c.day - interval '7' day
    and ac.day <= c.day - interval '1' day
)
select
  tt.day,
  u.user_type,
  u.flag_owns_team,
  fcm.company_id,
  count(1) as unique_users,
  sum(tt.active) as unique_active_users
from
  temp_table tt
  left join dm_okr.dim__user u on tt.user_id = u.id
  join {{ ref('fct__company_members') }} fcm on tt.user_id = fcm.user_id
where
  rn = 1
group by
  tt.day,
  u.user_type,
  u.flag_owns_team,
  fcm.company_id

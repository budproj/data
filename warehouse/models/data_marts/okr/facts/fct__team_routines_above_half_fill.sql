with calendar as (
  select
    distinct date_trunc('week', timestamp) as week
  from
    {{ ref('dim__answer_group') }} dag
  order by
    week
),
team_cross_week as (
  select
    *
  from
    {{ ref('dim__team') }} t
    join calendar c on t.created_at <= week
),
team_data as (
  select
    c.id,
    c.name,
    t.id as team_id,
    t.name as team_name,
    t.week,
    sum(
      case
        when t.id is not null then 1
        else 0
      end
    ) as active
  from
    {{ ref('dim__user') }} u
    left join {{ ref('fct__company_members') }} cm on u.id = cm.user_id
    left join {{ ref('dim__company') }} c on cm.company_id = c.id
    left join {{ ref('fct__team_members') }} tm on u.id = tm.user_id
    left join team_cross_week t on tm.team_id = t.id
    and (
      (
        u.status = 'INACTIVE'
        and t.week > u.created_at
        and t.week < u.updated_at
      )
      or (
        u.status = 'ACTIVE'
        and t.week > u.created_at
      )
    )
  group by
    c.id,
    c.name,
    t.id,
    t.name,
    t.week
),
routines_data as (
  select
    c.id,
    c.name,
    t.id as team_id,
    t.name as team_name,
    t.week,
    count(distinct dag.user_id) as routines
  from
    {{ ref('dim__answer_group') }} dag
    join {{ ref('dim__user') }} u on dag.user_id = u.id
    left join {{ ref('fct__company_members') }} cm on u.id = cm.user_id
    left join {{ ref('dim__company') }} c on cm.company_id = c.id
    left join {{ ref('fct__team_members') }} tm on u.id = tm.user_id
    left join team_cross_week t on tm.team_id = t.id
    and (
      (
        u.status = 'INACTIVE'
        and t.week > u.created_at
        and t.week < u.updated_at
      )
      or (
        u.status = 'ACTIVE'
        and t.week > u.created_at
      )
    )
  group by
    c.id,
    c.name,
    t.id,
    t.name,
    t.week
),
teams_routines_by_week as (
  select
    td.id,
    td.name,
    td.team_id,
    td.team_name,
    td.week,
    td.active,
    rd.routines,
    coalesce(rd.routines :: float / td.active, 0) as percent_fill_routines
  from
    team_data td
    left join routines_data rd on td.team_id = rd.team_id
    and rd.week = td.week
  order by
    percent_fill_routines asc
)
select
  week,
  sum(
    case
      when percent_fill_routines >= 0.5 then 1
      else 0
    end
  ) as teams_with_at_least_50_percent,
  count(team_id) as total
from
  teams_routines_by_week
group by
  week

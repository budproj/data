with calendar as (
  select
    distinct date_trunc('day', timestamp) as day
  from
    {{ ref('dim__answer_group') }} dag
  order by
    day
),
team_cross_day as (
  select
    *
  from
    {{ ref('dim__team') }} t
    join calendar c on t.created_at <= day
),
team_data as (
  select
    c.id,
    c.name,
    t.id as team_id,
    t.name as team_name,
    t.day,
    sum(
      case
        when t.id is not null then 1
        else 0
      end
    ) as active
  from
    team_cross_day t 
    left join {{ ref('fct__team_members') }} tm on t.id = tm.team_id
    left join {{ ref('dim__user') }} u on tm.team_id = t.id
    and (
      (
        u.status = 'INACTIVE'
        and t.day > u.created_at
        and t.day < u.updated_at
      )
      or (
        u.status = 'ACTIVE'
        and t.day > u.created_at
      )
    )
    left join {{ ref('fct__company_members') }} cm on u.id = cm.user_id
    left join {{ ref('dim__company') }} c on cm.company_id = c.id
  where
    c.name not in ('blah')
  group by
    c.id,
    c.name,
    t.id,
    t.name,
    t.day
)
select * from team_data

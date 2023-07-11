with first_event as (
    select
        date_trunc('day', timestamp) as day
    from
        {{ ref('dim__answer_group') }} dag
    order by
        timestamp asc
    limit
        1
), days_since_first_event as (
    select
        {{ days_in_range('day', 'current_date') }} as days_since_first_event
    from
        first_event
),
day_series as (
    select
        generate_series(0, days_since_first_event) as day_delta
    from
        days_since_first_event
),
calendar as (
    select
        date_trunc('day', day + interval '1 day' * day_delta) as day
    from
        day_series
        left join first_event on 1 = 1
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
                when u.id is not null then 1
                else 0
            end
        ) as active
    from
        team_cross_day t
        left join {{ ref('fct__team_members') }} tm on t.id = tm.team_id
        left join {{ ref('dim__user') }} u on tm.user_id = u.id
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
    group by
        c.id,
        c.name,
        t.id,
        t.name,
        t.day
    order by
        t.day
),
routines_data_wau as (
    select
        c.id,
        c.name,
        t.id as team_id,
        t.name as team_name,
        t.day,
        count(
            distinct (
                case
                    when dag.id is not null then u.id
                end
            )
        ) as weekly_active
    from
        team_cross_day t
        left join {{ ref('fct__team_members') }} tm on t.id = tm.team_id
        left join {{ ref('dim__user') }} u on tm.user_id = u.id
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
        left join {{ ref('dim__answer_group') }} dag on dag.user_id = u.id
        and dag.timestamp >= t.day - interval '7' day
        and dag.timestamp <= t.day
        left join {{ ref('fct__company_members') }} cm on u.id = cm.user_id
        left join {{ ref('dim__company') }} c on cm.company_id = c.id
    group by
        c.id,
        c.name,
        t.id,
        t.name,
        t.day
    order by
        t.day
),
routines_data_mau as (
    select
        c.id,
        c.name,
        t.id as team_id,
        t.name as team_name,
        t.day,
        count(
            distinct (
                case
                    when dag.id is not null then u.id
                end
            )
        ) as monthly_active
    from
        team_cross_day t
        left join {{ ref('fct__team_members') }} tm on t.id = tm.team_id
        left join {{ ref('dim__user') }} u on tm.user_id = u.id
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
        left join {{ ref('dim__answer_group') }} dag on dag.user_id = u.id
        and dag.timestamp >= t.day - interval '30' day
        and dag.timestamp <= t.day
        left join {{ ref('fct__company_members') }} cm on u.id = cm.user_id
        left join {{ ref('dim__company') }} c on cm.company_id = c.id
    group by
        c.id,
        c.name,
        t.id,
        t.name,
        t.day
    order by
        t.day
)
select
    team_data.*,
    routines_data_wau.weekly_active,
    routines_data_mau.monthly_active,
    routines_data_wau.weekly_active :: float / NULLIF(team_data.active, 0) as wau,
    routines_data_mau.monthly_active :: float / NULLIF(team_data.active, 0) as mau,
    routines_data_mau.monthly_active :: float / NULLIF(routines_data_wau.weekly_active, 0) as stickness
from
    team_data
    left join routines_data_wau on team_data.team_id = routines_data_wau.team_id
    and team_data.id = routines_data_wau.id
    and team_data.day = routines_data_wau.day
    left join routines_data_mau on team_data.team_id = routines_data_mau.team_id
    and team_data.id = routines_data_mau.id
    and team_data.day = routines_data_mau.day

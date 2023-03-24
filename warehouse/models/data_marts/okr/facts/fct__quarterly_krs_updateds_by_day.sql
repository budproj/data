with calendar as (
  select
    distinct day
  from
    {{ ref('fct__user_is_active_by_day') }} 
  order by
    day
),
team_calendar as (
    select
        day,
        t.id as team_id,
        t.company_id
    from
        calendar c  
        join {{ ref('dim__team') }}  t on c.day >= t.created_at
),
team_active_cycle as (
    select
        tc.day,
        tc.team_id,
        c.cadence,
        c.id as cycle_id
    from
        team_calendar tc
        left join {{ ref('dim__cycle') }} c on tc.day >= c.date_start and tc.day <= c.date_end and c.company_id = tc.company_id
),
krs_by_day as (
    select 
        tac.day,
        tac.team_id,
        count(distinct kr.id) as distinct_krs,
        count(distinct ci.key_result_id) as distinct_krs_checkined
    from 
        team_active_cycle tac
        join {{ ref('dim__key_result')}} kr on tac.team_id = kr.team_id and kr.cycle_id = tac.cycle_id
        left join {{ ref('dim__key_result_check_in')}} ci on kr.id = ci.key_result_id and ci.created_at >= tac.day :: date - 8 and ci.created_at <= tac.day :: date + 1
    where
        tac.cadence = 'QUARTERLY'
    group by
        tac.day,
        tac.team_id
)

select * from krs_by_day 

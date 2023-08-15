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
    join {{ ref('dim__team') }} t on c.day >= t.created_at
),
team_active_cycle as (
  select
    tc.day,
    tc.team_id,
    c.cadence,
    c.id as cycle_id,
    c.date_start,
    c.date_end
  from
    team_calendar tc
    left join {{ ref('dim__cycle') }} c on tc.day >= c.date_start
    and tc.day <= c.date_end
    and c.company_id = tc.company_id
),
cycles_with_expected_progress_by_week as (
  select
    *,
    (
      (
        date_trunc('week', day) :: date - date_trunc('week', date_start) :: date
      ) / 7
    ) :: float * 70 / 100 / (
      (
        date_trunc('week', date_end) :: date - date_trunc('week', date_start) :: date
      ) / 7
    ) as expected_progress
  from
    team_active_cycle
),
krs_per_cycles_with_expected_progress_by_week as (
  select
    cwepgbw.*,
    kr.id as key_result_id,
    kr.type
  from
    cycles_with_expected_progress_by_week cwepgbw
    left join {{ ref('dim__key_result') }} kr on cwepgbw.cycle_id = kr.cycle_id
    left join {{ ref('fct__key_result_latest_check_in') }} lci on kr.id = lci.key_result_id
    left join {{ ref('dim__key_result_check_in') }} krci on lci.key_result_check_in_id = krci.id
    left join {{ ref('dim__company') }} co on kr.company_id = co.id
  where
    krci.confidence <> -100 or
    krci.confidence is null or
    (krci.confidence = -100 and cwepgbw.day < krci.created_at)
),
check_in_by_week as (
  select
    krs_per_cycles_with_expected_progress_by_week.key_result_id,
    krs_per_cycles_with_expected_progress_by_week.team_id,
    krs_per_cycles_with_expected_progress_by_week.expected_progress,
    krs_per_cycles_with_expected_progress_by_week.cycle_id,
    date_trunc(
      'week',
      krs_per_cycles_with_expected_progress_by_week.day
    ) as week,
    case
      when krs_per_cycles_with_expected_progress_by_week.type = 'ASCENDING' then max(krci.value)
      when krs_per_cycles_with_expected_progress_by_week.type = 'DESCENDING' then min(krci.value)
      else null
    end as check_in_value
  from
    krs_per_cycles_with_expected_progress_by_week
    left join {{ ref('dim__key_result_check_in') }} krci on krs_per_cycles_with_expected_progress_by_week.key_result_id = krci.key_result_id
    and date_trunc(
      'day',
      krs_per_cycles_with_expected_progress_by_week.day
    ) = date_trunc('day', krci.created_at)
  group by
    krs_per_cycles_with_expected_progress_by_week.key_result_id,
    krs_per_cycles_with_expected_progress_by_week.type,
    date_trunc(
      'week',
      krs_per_cycles_with_expected_progress_by_week.day
    ),
    krs_per_cycles_with_expected_progress_by_week.team_id,
    krs_per_cycles_with_expected_progress_by_week.cycle_id,
    krs_per_cycles_with_expected_progress_by_week.expected_progress
),
temp_helper as (
  select
    key_result_id,
    team_id,
    week,
    expected_progress,
    cycle_id,
    check_in_value,
    sum(
      case
        when check_in_value is null then 0
        else 1
      end
    ) over (
      order by
        key_result_id,
        team_id,
        expected_progress,
        cycle_id,
        week
    ) as value_partition
  from
    check_in_by_week
  order by
    key_result_id,
    team_id,
    expected_progress,
    cycle_id,
    week
),
check_in_by_week_copy_last_non_null_value as (
  select
    key_result_id,
    team_id,
    expected_progress,
    cycle_id,
    week,
    first_value(check_in_value) over (
      partition by key_result_id,
      team_id,
      cycle_id,
      value_partition
      order by
        key_result_id,
        team_id,
        cycle_id,
        week
    ) as check_in_value
  from
    temp_helper
),
kr_percent_evolv as (
  select
    check_in_by_week_copy_last_non_null_value.key_result_id,
    check_in_by_week_copy_last_non_null_value.team_id,
    check_in_by_week_copy_last_non_null_value.expected_progress,
    check_in_by_week_copy_last_non_null_value.cycle_id,
    check_in_by_week_copy_last_non_null_value.week,
    check_in_by_week_copy_last_non_null_value.check_in_value,
    case
      when kr.goal = kr.initial_value then null
      when check_in_by_week_copy_last_non_null_value.check_in_value is null then 0
      when kr.type = 'ASCENDING' then 100 * (
        check_in_by_week_copy_last_non_null_value.check_in_value - kr.initial_value
      ) :: float / (kr.goal - kr.initial_value)
      when kr.type = 'DESCENDING' then 100 * (
        check_in_by_week_copy_last_non_null_value.check_in_value - kr.goal
      ) :: float / (kr.initial_value - kr.goal)
      else 0
    end as percent,
    kr.objective_id,
    kr.goal,
    kr.initial_value
  from
    check_in_by_week_copy_last_non_null_value
    left join {{ ref('dim__key_result') }} kr on check_in_by_week_copy_last_non_null_value.key_result_id = kr.id
),
weekly_kr_percent as (
  select
    kr_percent_evolv.*,
    case
      when percent < 0 then 0
      when percent > 100 then 100
      when percent is null then 0
      else percent
    end as limited_percent
  from
    kr_percent_evolv
)
select
  *
from
  weekly_kr_percent

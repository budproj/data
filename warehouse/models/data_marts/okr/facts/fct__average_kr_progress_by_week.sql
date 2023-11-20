with calendar as (
  select
    distinct date_trunc('week', day) as week
  from
    {{ ref('fct__user_is_active_by_day') }}
  where
    day > current_date - interval '365' day
  order by
    day
),
krs_per_cycles_with_expected_progress_by_week as (
  select
    c.week,
    t.id as team_id,
    cy.id as cycle_id,
    (
      (
        c.week :: date - date_trunc('week', cy.date_start) :: date
      ) / 7
    ) :: float * 70 / 100 / (
      (
        date_trunc('week', cy.date_end) :: date - date_trunc('week', cy.date_start) :: date
      ) / 7
    ) as expected_progress,
    kr.id as key_result_id,
    kr.type
  from
    calendar c
    join {{ ref('dim__team') }} t on c.week >= t.created_at
    left join {{ ref('dim__cycle') }} cy on c.week >= cy.date_start
    and c.week <= cy.date_end
    and cy.company_id = t.company_id
    left join {{ ref('dim__key_result') }} kr on cy.id = kr.cycle_id
    left join {{ ref('fct__key_result_latest_check_in') }} lci on kr.id = lci.key_result_id
    left join {{ ref('dim__key_result_check_in') }} krci on lci.key_result_check_in_id = krci.id
  where
    cy.cadence = 'QUARTERLY' and
    (krci.confidence <> -100 or
    krci.confidence is null or
    (krci.confidence = -100 and c.week < krci.created_at))
),
check_in_by_week as (
  select
    krs_per_cycles_with_expected_progress_by_week.key_result_id,
    krs_per_cycles_with_expected_progress_by_week.team_id,
    krs_per_cycles_with_expected_progress_by_week.expected_progress,
    krs_per_cycles_with_expected_progress_by_week.cycle_id,
    krs_per_cycles_with_expected_progress_by_week.week,
    LAST_VALUE (krci.value)  
        OVER ( 
            PARTITION BY krs_per_cycles_with_expected_progress_by_week.key_result_id, krs_per_cycles_with_expected_progress_by_week.week
            ORDER BY krci.created_at
    ) as check_in_value
  from
    krs_per_cycles_with_expected_progress_by_week
    left join {{ ref('dim__key_result_check_in') }} krci on krs_per_cycles_with_expected_progress_by_week.key_result_id = krci.key_result_id
    and krs_per_cycles_with_expected_progress_by_week.week = date_trunc('week', krci.created_at)
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
      partition by
        key_result_id
      order by
        week
    ) as value_partition
  from
    check_in_by_week
),
check_in_by_week_copy_last_non_null_value as (
  select
    key_result_id,
    team_id,
    expected_progress,
    cycle_id,
    week,
    first_value(check_in_value) over (
      partition by key_result_id
      order by
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

with users_active_kr as (
  select
    kr.id,
    kr.goal,
    kr.type,
    kr.format,
    kr.owner_id as user_id,
    kr.initial_value,
    kr.company_id
  from
    {{ ref('dim__key_result') }} kr
    left join {{ ref('dim__cycle') }} c on c.id = kr.cycle_id
  where
    c.cadence = 'QUARTERLY'
    and c.period = 'Q2'
    and date_trunc('quarter', c.date_start) = date_trunc('quarter', current_date)
),
distinct_users_with_krs as (
  select
    distinct user_id
  from
    users_active_kr
),
users_with_kr_by_week as (
  select
    user_by_week.user_id,
    user_by_week.week
  from
    {{ ref('fct__user_first_key_result_check_in_per_week') }} user_by_week
  where
    user_by_week.user_id in (
      select
        *
      from
        distinct_users_with_krs
    )
    and date_trunc('quarter', user_by_week.week) = date_trunc('quarter', current_date)
),
users_by_week_cross_kr as (
  select
    users_active_kr.user_id,
    users_with_kr_by_week.week,
    users_active_kr.id as key_result_id,
    users_active_kr.goal,
    users_active_kr.type,
    users_active_kr.initial_value,
    users_active_kr.format,
    users_active_kr.company_id
  from
    users_with_kr_by_week
    join users_active_kr on users_with_kr_by_week.user_id = users_active_kr.user_id
),
last_check_in_by_week_and_kr as (
  select
    *
  from
    (
      select
        krci.key_result_id,
        krci.user_id,
        krci.value,
        date_trunc('week', krci.created_at) as week,
        row_number() over (
          partition by krci.key_result_id,
          date_trunc('week', krci.created_at)
          order by
            krci.created_at desc
        ) as rn
      from
        {{ ref('dim__key_result_check_in') }} krci
    ) order_check_in_by_week_and_kr
  where
    rn = 1
),
user_kr_by_week as (
  select
    u.*,
    ci.value
  from
    users_by_week_cross_kr u
    left join last_check_in_by_week_and_kr ci on u.user_id = ci.user_id
    and u.key_result_id = ci.key_result_id
    and u.week = ci.week
),
final as (
  SELECT
    user_id,
    week,
    key_result_id,
    goal,
    type,
    initial_value,
    format,
    company_id,
    value,
    value_partition,
    first_value(value) over (
      partition by key_result_id,
      value_partition
      order by
        week
    )
  FROM
    (
      SELECT
        user_id,
        week,
        key_result_id,
        goal,
        type,
        initial_value,
        format,
        company_id,
        value,
        sum(
          case
            when value is null then 0
            else 1
          end
        ) over (
          partition by key_result_id
          order by
            week
        ) as value_partition
      FROM
        user_kr_by_week
      ORDER BY
        week ASC
    ) as q
)
select
    user_id,
    week,
    key_result_id,
    goal,
    type,
    initial_value,
    format,
    company_id,
    value_partition as value
from
  final
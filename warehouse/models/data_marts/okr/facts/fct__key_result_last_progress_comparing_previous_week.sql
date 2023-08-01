with key_result_progress_partited_by_week as (
  select
    *,
    row_number() over (
      partition by key_result_id,
      date_trunc('week', created_at)
      order by
        created_at desc
    ) as rn_kr_week
  from
    {{ ref('key_result_progress_record') }}
),
last_two_kr_progress as (
  select
    *,
    row_number() over (
      partition by key_result_id
      order by
        created_at desc
    ) as rn_week
  from
    key_result_progress_partited_by_week
  where
    rn_kr_week = 1
),
last_two_kr_progress_with_previous_progress_value as (
  select
    *,
    lag(progress, -1) over (
      partition by key_result_id
      order by
        created_at desc
    ) as previous_progress
  from
    last_two_kr_progress
  where
    rn_week = 1
    or rn_week = 2
),
kr_latest_progress_with_delta_with_previous_week as (
select
  id,
  key_result_id,
  date,
  progress as current_progress,
  created_at,
  updated_at,
  greatest(least(progress, 1.0), 0.0) - greatest(least(coalesce(previous_progress, 0.0), 1.0), 0.0) as weekly_delta_progress
from
  last_two_kr_progress_with_previous_progress_value
where
  rn_week = 1
)
select * from kr_latest_progress_with_delta_with_previous_week

with
  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  key_result_progress_evolution as (
    select * from {{ ref('fct__key_result_progress_evolution') }}
  ),

  evolution_with_day as (
    select
      *,
      date_trunc('day', date) as day
    from key_result_progress_evolution
  ),

  latest_report_by_day as (
    select
      key_result_id,
      day,
      max(date) as date
    from evolution_with_day
    group by key_result_id, day
  ),

  daily_grouped_evolution as (
    select
      lr.key_result_id,
      lr.day,
      ev.key_result_check_in_id,
      ev.progress
    from latest_report_by_day lr
    left join evolution_with_day ev on lr.date = ev.date and lr.key_result_id = ev.key_result_id
  ),

  final as (
    select
      key_result.id as key_result_id,
      daily_grouped_evolution.key_result_check_in_id as key_result_check_in_id,
      daily_grouped_evolution.day as day,
      daily_grouped_evolution.progress as progress
      from daily_grouped_evolution
      left join key_result on key_result.id = daily_grouped_evolution.key_result_id
  )

select * from final
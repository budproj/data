with
  cycle as (
    select * from {{ ref('dim__cycle') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  key_result_latest_check_in as (
    select * from {{ ref('fct__key_result_latest_check_in') }}
  ),

  key_result_check_in as (
    select * from {{ ref('dim__key_result_check_in') }}
  ),

  cycle_key_result_progress as (
    select
      cycle.id as cycle_id,
      {{ calculate_progress('key_result_check_in', 'key_result') }} as progress,
      key_result_check_in.created_at as date
      from cycle
      left join key_result on cycle.id = key_result.cycle_id
      left join key_result_latest_check_in on key_result.id = key_result_latest_check_in.key_result_id
      left join key_result_check_in on key_result_latest_check_in.key_result_check_in_id = key_result_check_in.id
  ),

  final as (
    select
      cycle_id,
      avg(progress) as progress,
      max(date) as date
      from cycle_key_result_progress
      group by cycle_id
  )

select * from final
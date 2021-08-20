with
  cycle as (
    select * from {{ ref('dim__cycle') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  key_result_progress as (
    select * from {{ ref('fct__key_result_progress') }}
  ),

  cycle_key_result_progress as (
    select
      cycle.id as cycle_id,
      key_result_progress.progress,
      key_result_progress.date
      from cycle
      left join key_result on cycle.id = key_result.cycle_id
      left join key_result_progress on key_result.id = key_result_progress.key_result_id
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
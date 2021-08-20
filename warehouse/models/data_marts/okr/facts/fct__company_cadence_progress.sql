with
  company as (
    select * from {{ ref('dim__company') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  cycle as (
    select * from {{ ref('dim__cycle') }}
  ),

  key_result_progress as (
    select * from {{ ref('fct__key_result_progress') }}
  ),

  company_key_result_progress as (
    select
      company.id as company_id,
      cycle.id as cycle_id,
      cycle.date_start as cycle_date_start,
      cycle.cadence as cadence,
      key_result_progress.progress,
      key_result_progress.date
      from company
      left join key_result on company.id = key_result.team_id
      left join key_result_progress on key_result.id = key_result_progress.key_result_id
      left join cycle on key_result.cycle_id = cycle.id
  ),

  company_progress_in_cycle as (
    select
      company_id,
      cycle_id,
      cycle_date_start,
      cadence,
      avg(progress) as progress,
      max(date) as date
      from company_key_result_progress
      group by company_id, cycle_id, cycle_date_start, cadence
  ),

  company_ranked_cadence as (
    select
      *,
      row_number() over (partition by company_id, cadence order by cycle_date_start desc) as rank
      from company_progress_in_cycle
  ),

  final as (
    select
      company_id,
      cycle_id,
      cadence,
      progress,
      date
      from company_ranked_cadence
      where rank = 1
  )

select * from final
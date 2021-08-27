with
  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  stg_analytics__key_result_progress_record as (
    select * from {{ ref('stg_analytics__key_result_progress_record') }}
  ),

  final as (
    select
      key_result.id as key_result_id,
      pr.id as key_result_check_in_id,
      pr.progress,
      pr.created_at as date
      from stg_analytics__key_result_progress_record as pr
      left join key_result on key_result.id = pr.key_result_id
  )

select * from final
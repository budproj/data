with
  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  stg_analytics__key_result_progress_record as (
    select * from {{ ref('stg_analytics__key_result_progress_record') }}
  ),

  ranked_progress_record as (
    select
      stg_analytics__key_result_progress_record.*,
      {{ row_number('key_result_id', 'created_at', 'desc') }} as rank
    from stg_analytics__key_result_progress_record
  ),

  latest_progress_record as (
    select
      *
    from ranked_progress_record
    where rank = 1
  ),

  final as (
    select
      key_result.id as key_result_id,
      coalesce(latest_progress_record.progress, 0) as progress,
      coalesce(latest_progress_record.date, now()) as date
      from key_result
      left join latest_progress_record on key_result.id = latest_progress_record.key_result_id
  )

select * from final

with
  src_analytics__key_result_progress_record as (
    select * from {{ ref('key_result_progress_record') }}
  ),

  final as (
    select
      id::text,
      key_result_id::uuid,
      key_result_check_in_id::uuid,
      date::timestamp,
      progress::float,
      created_at::timestamp,
      updated_at::timestamp
    from src_analytics__key_result_progress_record
  )

select * from final

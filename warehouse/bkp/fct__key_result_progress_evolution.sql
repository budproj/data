with
  key_result_check_in as (
    select * from {{ ref('dim__key_result_check_in') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  final as (
    select
      key_result.id as key_result_id,
      key_result_check_in.id as key_result_check_in_id,
      {{ calculate_progress('key_result_check_in', 'key_result') }} as progress,
      key_result_check_in.created_at as date
      from key_result
      left join key_result_check_in on key_result.id = key_result_check_in.key_result_id
  )

select * from final
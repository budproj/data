with
  key_result_check_in as (
    select * from {{ ref('stg_okr__key_result_check_in') }}
  ),

  key_result as (
    select * from {{ ref('stg_okr__key_result') }}
  ),

  final as (
    select
      {{ generate_uuid() }} as id,
      key_result.id as key_result_id,
      key_result_check_in.id as key_result_check_in_id,
      key_result_check_in.created_at as date,
      {{ calculate_progress('key_result_check_in', 'key_result') }} as progress,
      key_result_check_in.created_at,
      key_result_check_in.created_at as updated_at
    from key_result_check_in
    left join key_result on key_result_check_in.key_result_id = key_result.id
  )

select * from final
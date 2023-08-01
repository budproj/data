with
  stg_okr__key_result_check_in as (
    select * from {{ ref('stg_okr__key_result_check_in') }}
  ),

  final as  (
    select
      id,
      key_result_id,
      value,
      created_at,
      created_at as updated_at
    from stg_okr__key_result_check_in
  )

select * from final

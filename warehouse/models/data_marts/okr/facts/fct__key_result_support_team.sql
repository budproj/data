with
  stg_okr__key_result_support_team as (
    select * from {{ ref('stg_okr__key_result_support_team') }}
  ),

  final as (
    select key_result_id, user_id from stg_okr__key_result_support_team
  )

select * from final

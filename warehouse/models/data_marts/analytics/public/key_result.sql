with
  stg_okr__key_result as (
    select * from {{ ref('stg_okr__key_result') }}
  ),

  final as  (
    select
      id,
      initial_value,
      goal,
      type,
      created_at,
      updated_at
    from stg_okr__key_result
  )

select * from final
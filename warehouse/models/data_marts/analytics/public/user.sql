with
  stg_okr__user as (
    select * from {{ ref('stg_okr__user') }}
  ),

  final as (
    select
      id,
      created_at,
      updated_at
    from stg_okr__user
  )

select * from final
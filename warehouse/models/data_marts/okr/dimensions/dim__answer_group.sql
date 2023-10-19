with
  stg_routines__answer_group as (
    select * from {{ ref('stg_routines__answer_group') }}
  ),

  final as (
    select
      id,
      "userId" as user_id,
      "companyId"  as company_id,
      timestamp
    from stg_routines__answer_group
  )

select * from final

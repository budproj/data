with
  stg_routines__answer_group as (
    select * from {{ ref('stg_routines__answer_group') }}
  ),

  final as (
    select
      id,
      userid as user_id,
      companyid  as company_id,
      timestamp
    from stg_routines__answer_group
  )

select * from final
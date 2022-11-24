with
  src_routines__answer_group as (
    select * from {{ source('conformed', 'routines__answergroup') }}
  ),

  final as (
    select
      id :: uuid,
      userid ::uuid,
      companyid::uuid,
      timestamp::timestamp
    from src_routines__answer_group
  )

select * from final

with
  src_routines__answer_group as (
    select * from {{ source('routines', 'AnswerGroup') }}
  ),

  final as (
    select
      id :: uuid,
      "userId" ::uuid,
      "companyId"::uuid,
      timestamp::timestamp
    from src_routines__answer_group
  )

select * from final

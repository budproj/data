with
  stg_routines__answer as (
    select * from {{ ref('stg_routines__answer') }}
  ),

  final as (
    select
      id,
      value,
      "questionId" as question_id,
      "answerGroupId" as answer_group_id
    from stg_routines__answer
  )

select * from final

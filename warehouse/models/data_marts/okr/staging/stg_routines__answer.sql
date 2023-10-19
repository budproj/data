with
  src_routines__answer as (
    select * from {{ source('routines', 'Answer') }}
  ),

  final as (
    select
      id :: uuid,
      value :: varchar(256),
      "questionId"::uuid,
      "answerGroupId"::uuid
    from src_routines__answer where value is not null
  )

select * from final

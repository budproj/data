with
  stg_routines__answer as (
    select * from {{ ref('stg_routines__answer') }}
  ),

  final as (
    select
      id,
      value,
      questionid  as question_id,
      answergroupid as answer_group_id
    from stg_routines__answer
  )

select * from final
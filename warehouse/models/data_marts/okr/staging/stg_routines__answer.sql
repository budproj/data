with
  src_routines__answer as (
    select * from {{ source('conformed', 'routines__answer') }}
  ),

  final as (
    select
      id :: uuid,
      value :: varchar(256),
      questionid::uuid,
      answergroupid::uuid
    from src_routines__answer where value is not null
  )

select * from final

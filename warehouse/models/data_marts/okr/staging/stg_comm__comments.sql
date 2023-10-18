with
  src__comments as (
    select * from {{ source('comments', 'comment') }}
  ),

  final as (
    select 
      id :: uuid,
      entity :: text,
      userid :: uuid,
      content :: text,
      createdat :: timestamp
    from src__comments
  )

select * from final

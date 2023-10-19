with
  src__comments as (
    select * from {{ source('comments', 'Comment') }}
  ),

  final as (
    select 
      id :: uuid,
      entity :: text,
      "userId" :: uuid,
      content :: text,
      "createdAt" :: timestamp
    from src__comments
  )

select * from final

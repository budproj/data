with
  stg_comm__comments as (
    select * from {{ ref('stg_comm__comments') }}
  ),

  final as (
    select
        id,
        split_part(split_part(entity, 'routine:', 2), ':', 1) as first_entity,
        "userId" as user_id,
        content,
        "createdAt" as created_at
    from stg_comm__comments
  )

select * from final

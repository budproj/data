with
  user_accesses as (
    select * from {{ ref('fct__user_accesses') }}
  ),

  user_last_access as (
    select
      user_id,
      max(access_time) as last_access_time
    from user_accesses
    group by user_id
  ),

  final as (
    select
      user_accesses.user_id,
      amplitude_event_id
    from user_last_access
    left join user_accesses on user_accesses.access_time = user_last_access.last_access_time
  )

select * from final
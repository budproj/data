with
  amplitude_event as (
    select * from {{ ref('dim__amplitude_event') }}
  ),

  amplitude_event_with_rank as (
    select
      *,
      row_number() over (partition by user_id, session_id order by event_time asc) as rank
      from amplitude_event
  ),

  accesses as (
    select
      id as amplitude_event_id,
      user_id,
      session_id,
      event_time as access_time
      from amplitude_event_with_rank
      where
        event_type = 'PageView' and
        rank = 1
  ),

  final as (
    select * from accesses
  )

select * from final
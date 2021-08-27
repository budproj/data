with
  amplitude_event as (
    select * from {{ ref('dim__amplitude_event') }}
  ),

  final as (
    select
      user_id,
      id as amplitude_event_id,
      event_time as access_time
      from amplitude_event
      where event_type = 'PageView'
  )

select * from final
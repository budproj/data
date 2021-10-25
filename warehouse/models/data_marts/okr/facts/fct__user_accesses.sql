with
  amplitude_event as (
    select * from {{ ref('dim__amplitude_event') }}
  ),

  final as (
    select
      a1.user_id,
      a1.session_id,
      a2.id as amplitude_event_id,
      a2.event_time as access_time
      from amplitude_event a1
      left outer join lateral (
        select
          *
          from amplitude_event a2
          where
            a2.event_type = 'PageView' and
            a1.user_id = a2.user_id and
            a1.session_id = a2.session_id
          order by a2.event_time
          limit 1
      ) as a2 on true
      where a1.event_type = 'PageView'
      group by a1.user_id, a1.session_id, a2.id, a2.event_time
  )

select * from final
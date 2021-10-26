with
  stg_amplitude__events as (
    select * from {{ ref('stg_amplitude__events') }}
  ),

  final as (
    select
      uuid as id,
      user_id,
      session_id,
      event_time,
      event_type,
      event_properties
    from stg_amplitude__events
  )

select * from final
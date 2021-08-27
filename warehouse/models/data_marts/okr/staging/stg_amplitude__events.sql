with
  src_amplitude__events as (
    select * from {{ source('conformed', 'amplitude__events') }}
  ),

  final as (
    select
      app::bigint,
      dma::varchar(128),
      adid::varchar(128),
      city::varchar(256),
      data::json,
      idfa::varchar(128),
      uuid::uuid,
      groups::json,
      paying::boolean,
      region::varchar(256),
      country::varchar(256),
      library::varchar(128),
      os_name::varchar(32),
      user_id::uuid,
      event_id::bigint,
      language::varchar(32),
      platform::varchar(32),
      device_id::varchar(256),
      event_time::timestamp,
      event_type::varchar(32),
      ip_address::varchar(32),
      os_version::varchar(32),
      session_id::bigint,
      device_type::varchar(32),
      sample_rate::json,
      amplitude_id::bigint,
      device_brand::varchar(32),
      device_model::varchar(32),
      location_lat::real,
      location_lng::real,
      version_name::varchar(32),
      device_family::varchar(32),
      start_version::varchar(32),
      device_carrier::varchar(32),
      processed_time::timestamp,
      user_properties::json,
      event_properties::json,
      group_properties::json,
      client_event_time::timestamp,
      client_upload_time::timestamp,
      server_upload_time::timestamp,
      user_creation_time::timestamp,
      device_manufacturer::varchar(32),
      amplitude_event_type::varchar(32),
      is_attribution_event::boolean,
      server_received_time::timestamp,
      amplitude_attribution_ids::varchar(32)
    from src_amplitude__events where user_id is not null
  )

select * from final
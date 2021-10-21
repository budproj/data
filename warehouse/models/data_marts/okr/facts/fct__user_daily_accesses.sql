with
  user_accesses as (
    select * from {{ ref('fct__user_accesses') }}
  ),

  users as (
    select * from {{ ref('dim__user') }}
  ),

  user_days_since_creation as (
    select
      id as user_id,
      date_part('day', current_date::timestamp - created_at::timestamp)::integer as days_since_creation
    from users
  ),

  user_days_delta as (
    select
      user_days_since_creation.*,
      generate_series(1, user_days_since_creation.days_since_creation) as days_delta
    from user_days_since_creation
  ),

  user_days as (
    select
      user_days_delta.user_id,
      date_trunc('day', current_date - user_days_delta.days_delta) as day
    from user_days_delta
  ),

  user_accesses_truncated_by_day as (
    select
      user_accesses.*,
      date_trunc('day', user_accesses.access_time) as day
    from user_accesses
  ),

  user_days_with_first_access as (
    select
      user_days.*,
      user_accesses.amplitude_event_id as first_access_amplitude_event_id
      from user_days
      left outer join lateral (
        select
          *
          from user_accesses_truncated_by_day
          where
            user_accesses_truncated_by_day.user_id = user_days.user_id and
            user_accesses_truncated_by_day.day = user_days.day
          order by user_accesses_truncated_by_day.access_time asc
          limit 1
      ) as user_accesses on true
  ),

  final as (
    select * from user_days_with_first_access order by day desc
  )

select * from final
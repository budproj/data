with
  amplitude_events as (
    select * from {{ ref('dim__amplitude_event') }}
  ),

  first_event as (
    select
      *
      from amplitude_events
      order by event_time asc
      limit 1
  ),

  months_since_first_event as (
    select
      {{ months_in_range('event_time', 'current_date') }} as months_since_first_event
    from first_event
  ),

  month_series as (
    select
      generate_series(0, months_since_first_event) as month_delta
    from months_since_first_event
  ),

  months as (
    select
      date_trunc('month', event_time + interval '1 month' * month_delta) as month
    from month_series
    left join first_event on 1=1
  ),

  unique_users_per_month as (
    select
      date_trunc('month', event_time) as month,
      count(distinct user_id) as unique_users
    from amplitude_events
    group by date_trunc('month', event_time)
  ),

  final as (
    select
      months.month as month,
      coalesce(unique_users_per_month.unique_users, 0) as amount_of_active_users
    from months
    left join unique_users_per_month on unique_users_per_month.month = months.month
  )

select * from final

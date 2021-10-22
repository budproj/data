with
  user_first_access_per_day as (
    select * from {{ ref('fct__user_first_access_per_day') }}
  ),

  user_day_with_access_flag as (
    select
      *,
      case when first_access_amplitude_event_id is null then 0 else 1 end as access_flag
      from user_first_access_per_day
  ),

  user_with_daily_access_average as (
    select
      user_id,
      avg(access_flag) as average
      from user_day_with_access_flag
      group by user_id
  ),

  final as (
    select * from user_with_daily_access_average
  )

select * from final
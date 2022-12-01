with
  user_accesses as (
    select * from {{ ref('fct__user_accesses') }}
  ),

  users as (
    select * from {{ ref('dim__user') }}
  ),

  user_months_since_creation as (
    select
      id as user_id,
      {{ months_in_range('created_at', 'current_date') }} as months_since_creation
    from users
  ),

  user_months_delta as (
    select
      user_months_since_creation.*,
      generate_series(1, user_months_since_creation.months_since_creation) as months_delta
    from user_months_since_creation
  ),

  user_months as (
    select
      user_months_delta.user_id,
      date_trunc('month', current_date)  + interval '-1 month' * user_months_delta.months_delta  as month
    from user_months_delta
  ),

  user_accesses_truncated_by_month as (
    select
      user_accesses.*,
      date_trunc('month', user_accesses.access_time) as month
    from user_accesses
  ),

  user_accesses_with_rank as (
    select
      *,
      row_number() over (partition by user_id, month order by access_time asc) as rank
      from user_accesses_truncated_by_month
  ),

  user_first_monthly_access as (
    select
      *
      from user_accesses_with_rank
      where rank = 1
  ),

  user_months_with_first_access as (
    select
      user_months.*,
      user_first_monthly_access.amplitude_event_id as first_access_amplitude_event_id
      from user_months
      left join user_first_monthly_access on
        user_months.user_id = user_first_monthly_access.user_id and
        user_months.month = user_first_monthly_access.month
  ),

  final as (
    select * from user_months_with_first_access order by month desc
  )

select * from final
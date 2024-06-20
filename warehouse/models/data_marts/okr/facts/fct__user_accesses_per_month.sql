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
      date_trunc('month', current_date - user_months_delta.months_delta * 7) as month
    from user_months_delta
  ),

  user_accesses_truncated_by_month as (
    select
      user_accesses.*,
      date_trunc('month', user_accesses.access_time) as month
    from user_accesses
  ),

  user_months_with_accesses_count as (
    select
      user_months.user_id,
      user_months.month,
      count(*) filter (where user_accesses_truncated_by_month.access_time is not null) as accesses
      from user_months
      left join user_accesses_truncated_by_month on
        user_months.user_id = user_accesses_truncated_by_month.user_id and
        user_months.month = user_accesses_truncated_by_month.month
      group by user_months.user_id, user_months.month
  ),

  final as (
    select * from user_months_with_accesses_count order by month desc
  )

select * from final

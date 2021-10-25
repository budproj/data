with
  user_key_result_check_ins as (
    select * from {{ ref('fct__user_key_result_check_ins') }}
  ),

  users as (
    select * from {{ ref('dim__user') }}
  ),

  user_weeks_since_creation as (
    select
      id as user_id,
      {{ weeks_in_range('created_at', 'current_date') }} as weeks_since_creation
    from users
  ),

  user_weeks_delta as (
    select
      user_weeks_since_creation.*,
      generate_series(1, user_weeks_since_creation.weeks_since_creation) as weeks_delta
    from user_weeks_since_creation
  ),

  user_weeks as (
    select
      user_weeks_delta.user_id,
      date_trunc('week', current_date - user_weeks_delta.weeks_delta * 7) as week
    from user_weeks_delta
  ),

  user_key_result_check_ins_truncated_by_week as (
    select
      user_key_result_check_ins.*,
      date_trunc('week', user_key_result_check_ins.date) as week
    from user_key_result_check_ins
  ),

  user_weeks_with_first_check_in as (
    select
      user_weeks.*,
      user_key_result_check_ins.key_result_check_in_id as first_key_result_check_in_id
      from user_weeks
      left outer join lateral (
        select
          *
          from user_key_result_check_ins_truncated_by_week
          where
            user_weeks.user_id = user_key_result_check_ins_truncated_by_week.user_id and
            user_weeks.week = user_key_result_check_ins_truncated_by_week.week
          order by user_key_result_check_ins_truncated_by_week.date asc
          limit 1
      ) as user_key_result_check_ins on true
  ),

  final as (
    select * from user_weeks_with_first_check_in order by week desc
  )

select * from final
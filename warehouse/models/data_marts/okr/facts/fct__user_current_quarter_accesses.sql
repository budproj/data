with
  user_accesses as (
    select * from {{ ref('fct__user_accesses') }}
  ),

  user_accesses_with_quarter as (
    select
      *,
      extract(quarter from access_time) as quarter,
      extract(year from access_time) as year
      from user_accesses
  ),

  final as (
    select
      amplitude_event_id,
      user_id,
      access_time
      from user_accesses_with_quarter
      where
        year=extract(year from current_date)
        and quarter=extract(quarter from current_date)
  )

select * from final
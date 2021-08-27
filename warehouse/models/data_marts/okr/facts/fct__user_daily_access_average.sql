with
  user_access as (
    select * from {{ ref('fct__user_accesses') }}
  ),

  users as (
    select * from {{ ref('dim__user') }}
  ),

  user_days_since_creation as (
    select
      id as user_id,
      date_part('day', current_date::timestamp - created_at::timestamp) as days_since_creation
    from users
  ),

  user_number_of_accesses as (
    select
      user_id,
      count(user_id) as number_of_accesses
    from user_access
    group by user_id
  ),

  final as (
    select
      ud.user_id,
      coalesce(
        (
          ua.number_of_accesses/
          coalesce(
            nullif(ud.days_since_creation,0),
            1
          )
        ),
        0
      ) as average_access_per_day
    from user_days_since_creation ud
    left join user_number_of_accesses ua on ua.user_id = ud.user_id
  )

select * from final
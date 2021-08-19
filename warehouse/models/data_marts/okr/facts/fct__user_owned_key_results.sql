with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  user_owned_key_result as (
    select
      owner_id as user_id,
      count(owner_id) as number_of_owned_key_results
    from key_result
    group by owner_id
  ),

  final as (
    select
      users.id as user_id,
      coalesce(number_of_owned_key_results, 0) as owned_key_results
    from users
    left join user_owned_key_result on users.id = user_owned_key_result.user_id
  )

select * from final
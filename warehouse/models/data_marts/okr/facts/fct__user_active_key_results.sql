with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  cycle as (
    select * from {{ ref('dim__cycle') }}
  ),

  final as (
    select
      users.id as user_id,
      key_result.id as key_result_id
      from users
      left join key_result on users.id = key_result.owner_id
      left join cycle on key_result.cycle_id = cycle.id
      where cycle.active = true
  )

select * from final
with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  key_result_check_mark as (
    select * from {{ ref('dim__key_result_check_mark') }}
  ),

  cycle as (
    select * from {{ ref('dim__cycle') }}
  ),


  active_key_results as (
    select
      key_result.id as id,
      key_result.owner_id as owner_id
      from key_result
      left join cycle on cycle.id = key_result.cycle_id
      where cycle.active = true
  ),

  grouped_check_marks as (
    select
      key_result_id,
      count(*) as amount_of_check_marks
    from key_result_check_mark
    group by key_result_id
  ),

  final as (
    select
      users.id as user_id,
      coalesce(sum(amount_of_check_marks), 0) as amount_of_active_check_marks
    from users
    left join active_key_results on active_key_results.owner_id = users.id
    left join grouped_check_marks on grouped_check_marks.key_result_id = active_key_results.id
    group by user_id
  )

select * from final

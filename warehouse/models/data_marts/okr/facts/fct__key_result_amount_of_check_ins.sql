with
  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  key_result_check_in as (
    select * from {{ ref('dim__key_result_check_in') }}
  ),

  grouped_check_ins as (
    select
      key_result_id,
      count(*) as amount_of_check_ins
    from key_result_check_in
    group by key_result_id
  ),

  final as (
    select
      key_result.id as key_result_id,
      coalesce(amount_of_check_ins, 0) as amount_of_check_ins
    from key_result
    left join grouped_check_ins on grouped_check_ins.key_result_id = key_result.id
  )

select * from final
with
  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  key_result_check_mark as (
    select * from {{ ref('dim__key_result_check_mark') }}
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
      key_result.id as key_result_id,
      coalesce(amount_of_check_marks, 0) as amount_of_check_marks
    from key_result
    left join grouped_check_marks on grouped_check_marks.key_result_id = key_result.id
  )

select * from final

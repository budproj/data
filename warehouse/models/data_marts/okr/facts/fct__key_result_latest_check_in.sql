with
  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  key_result_check_in as (
    select * from {{ ref('dim__key_result_check_in') }}
  ),

  ranked_key_result_check_in as (
    select
      *,
      row_number() over (partition by key_result_id order by created_at desc) as rank
      from key_result_check_in
  ),

  final as (
    select
      key_result.id as key_result_id,
      ranked_key_result_check_in.id as key_result_check_in_id
    from key_result
    left join ranked_key_result_check_in on ranked_key_result_check_in.key_result_id = key_result.id
    where ranked_key_result_check_in.rank = 1
  )

select * from final

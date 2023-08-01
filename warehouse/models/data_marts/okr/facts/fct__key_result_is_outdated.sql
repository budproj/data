with
  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  key_result_latest_check_in as (
    select * from {{ ref('fct__key_result_latest_check_in') }}
  ),

  key_result_check_in as (
    select * from {{ ref('dim__key_result_check_in') }}
  ),

  final as (
    select
      key_result.id as key_result_id,
      key_result_check_in.id as key_result_check_in_id,
      (
        case
          when key_result_check_in.id is null then true
          else
            key_result_check_in.created_at <
            {{ get_start_of_week() }}
          end
      ) as is_outdated,
      key_result_check_in.created_at as date
      from key_result
      left join key_result_latest_check_in on key_result.id = key_result_latest_check_in.key_result_id
      left join key_result_check_in on key_result_latest_check_in.key_result_check_in_id = key_result_check_in.id
  )

select * from final

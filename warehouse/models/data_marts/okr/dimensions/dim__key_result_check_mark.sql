with
  stg_okr__key_result_check_mark as (
    select * from {{ ref('stg_okr__key_result_check_mark') }}
  ),

  key_result as (
    select * from {{ ref('dim__key_result') }}
  ),

  final as  (
    select
      stg_okr__key_result_check_mark.*,
      key_result.objective_id as objective_id,
      key_result.cycle_id as cycle_id,
      key_result.team_id as team_id,
      key_result.company_id as company_id
      from stg_okr__key_result_check_mark
      left join key_result on stg_okr__key_result_check_mark.key_result_id = key_result.id
  )

select * from final

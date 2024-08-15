with
  stg_routines__answer_group as (
    select * from {{ ref('stg_routines__answer_group') }}
  ),

  answers as (
    select * from {{ ref('dim__answers') }}
  ),

  final as (
    select
      ag.id,
      ag."userId" as user_id,
      ag."companyId"  as company_id,
      MAX(CASE WHEN a.question_id = 'd81e7754-79be-4638-89f3-a74875772d00' THEN a.value END) as feeling_reason,
      MAX(CASE WHEN a.question_id = 'f0c6e297-7eb7-4b48-869c-aec96240ba2b' THEN a.value END) as productivity_reason,
      MAX(CASE WHEN a.question_id = '95b84e67-d5b6-4fcf-938a-b4c9897596cb' THEN a.value END) as done_work,
      MAX(CASE WHEN a.question_id = 'a1d5b993-9430-40bb-8f0f-47cda69720b9' THEN a.value END) as priorities,
      MAX(CASE WHEN a.question_id = 'd9ca02f3-7bf7-40f3-b393-618de3410751' THEN a.value END) as block_reason,
      MAX(CASE WHEN a.question_id = 'fd7c26dd-38e3-41e7-b24a-78030653dc23' THEN a.value END) as message,
      timestamp
    from stg_routines__answer_group ag
    left join answers a on a.answer_group_id = ag.id
    group by ag.id, ag."userId", ag."companyId", timestamp
  )

select * from final

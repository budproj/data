with
  answer_groups as (
    select * from {{ ref('dim__answer_group') }}
  ),

  answers as (
    select * from {{ ref('dim__answers') }}
  ),

  final as (
    select
      ag.id,
      MAX(CASE WHEN a.question_id = '44bd7498-e528-4f96-b45e-3a2374790373' THEN a.value END) as feeling_value,
      MAX(CASE WHEN a.question_id = '9a56911a-61c1-49af-87a8-7a35a1804f6b' THEN a.value END) as productivity_value,
      MAX(CASE WHEN a.question_id = 'cf785f20-5a0b-4c4c-b882-9e3949589df2' THEN a.value END) as blockers_value
    from answer_groups ag
    left join answers a on a.answer_group_id = ag.id
    group by ag.id
  )

select * from final

with recursive
  stg_okr__team as (
    select * from {{ ref('stg_okr__team') }} where not (id :: text = any(ARRAY {{ var('sandbox_ids') }}))
  ),

  company as (
    select * from {{ ref('company') }}
  ),

  team_parents(id, parent_id, root_id) as (
    -- Get root nodes
    select id, parent_id, id as root_id
    from stg_okr__team
    where parent_id is null

    union all

    -- Get children  
    select t.id, t.parent_id, p.root_id
    from team_parents p
    join stg_okr__team t
    on t.parent_id = p.id
  ),

  team as (
    select 
      id,
      created_at,
      updated_at
    from stg_okr__team
  ),

  final as (
    select
      team.*,
      company.id as company_id
      from team
    left join team_parents on team_parents.id = team.id
    left join company on company.team_id = team_parents.root_id
  )

select * from final

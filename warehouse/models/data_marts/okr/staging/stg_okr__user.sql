with
  src_okr__user as (
    select * from {{ source('conformed', 'okr__user') }}
  ),

  final as (
    select
      id::uuid,
      role,
      about::text,
      email::varchar(256),
      gender::varchar(32),
      picture::text,
      nickname::varchar(256),
      authz_sub::char(30),
      first_name::varchar(256),
      last_name::varchar(256),
      linked_in_profile_address::text,
      status::varchar(32),
      created_at::timestamp,
      updated_at::timestamp
    from src_okr__user
  )

select * from final
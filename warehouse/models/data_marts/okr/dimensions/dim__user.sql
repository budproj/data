with stg_okr__user as (
  select
    *
  from
    {{ ref('stg_okr__user') }}
),
amplitude_event as (
  select
    *
  from
    {{ ref('dim__amplitude_event') }}
),
buddies_accross_others_companies as (
    select
        email
    from
        {{ ref('stg_okr__user') }} u
        left join {{ ref('stg_analytics__company_users_user') }}  cuu on u.id = cuu.user_id
        left join {{ ref('stg_analytics__company') }} c on cuu.company_id = c.id
    where
        c.name <> 'weme'
        and u.email like  '%@getbud.co'
),
users as (
  select
    *
  from
    stg_okr__user
),
users_with_type as (
  select
    users.*,
    case
      when (
          select
            count(*)
          from
            buddies_accross_others_companies
          where
            email = users.email
      ) = 1 then 'CS'
      else 'CUSTOMER'
    end as type
  from
    users
),
user_owner_okr as (
  select
    u.id,
    count(distinct kr.id) as count_okrs
  from
    users_with_type u
    join postgres_business.key_result kr on u.id :: text = kr.owner_id
  group by
    u.id
),
user_supports_okr as (
  select
    u.id,
    count(distinct stm.key_result_id) as count_okrs
  from
    users_with_type u
    join postgres_business.key_result_support_team_members_user stm on u.id :: text = stm.user_id
  group by
    u.id
),
user_owns_team as (
  select
    u.id,
    count(1) as count_teams
  from
    users_with_type u
    join {{ ref('dim__team') }} t on u.id = t.owner_id
  group by
    u.id
),
final as (
  select
    u.*,
    case
      when uoo.count_okrs is not null then 'OKR owner'
      when uso.count_okrs is not null then 'Supports OKR'
      else 'Isnt related to any okr'
    end as user_type,
    case
      when uot.count_teams is not null then true
      else false
    end as flag_owns_team
  from
    users_with_type u
    left join user_owner_okr uoo on u.id = uoo.id
    left join user_supports_okr uso on u.id = uso.id
    left join user_owns_team uot on u.id = uot.id
  where u.type <> 'CS'
)
select
  *
from
  final
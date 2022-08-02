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
seed_buddy_users_email as (
  select
    email
  from
    {{ ref('buddy_users') }}
),
seed_sandbox_users_email as (
  select
    email
  from
    {{ ref('sandbox_users') }}
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
          seed_buddy_users_email
        where
          email = users.email
      ) = 1 then 'BUDDY'
      when (
        select
          count(*)
        from
          seed_sandbox_users_email
        where
          email = users.email
      ) = 1 then 'SANDBOX'
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
    join conformed.okr__key_Result kr on u.id :: text = kr.owner_id
  group by
    u.id
),
user_supports_okr as (
  select
    u.id,
    count(distinct stm.key_result_id) as count_okrs
  from
    users_with_type u
    join conformed.okr__key_result_support_team_members_user stm on u.id :: text = stm.user_id
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
)
select
  *
from
  final
with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  user_access as (
    select * from {{ ref('fct__user_accesses') }}
  ),

  key_result_check_in as (
    select * from {{ ref('dim__key_result_check_in') }}
  ),

  key_result_comment as (
    select * from {{ ref('dim__key_result_comment') }}
  ),

  ranked_user_access as (
    select
      *,
      row_number() over (partition by user_id order by access_time asc) as rank
      from user_access
  ),

  ranked_key_result_check_in as (
    select
      *,
      row_number() over (partition by user_id order by created_at asc) as rank
      from key_result_check_in
  ),

  ranked_key_result_comment as (
    select
      *,
      row_number() over (partition by user_id order by created_at asc) as rank
      from key_result_comment
  ),

  user_with_first_access as (
    select
      users.id as user_id,
      ranked_user_access.amplitude_event_id as first_access_amplitude_event_id
      from users
      left join ranked_user_access on users.id = ranked_user_access.user_id
      where rank = 1 or rank is null
  ),

  user_with_first_check_in as (
    select
      user_with_first_access.user_id,
      first_access_amplitude_event_id,
      ranked_key_result_check_in.id as first_key_result_check_in_id
      from user_with_first_access
      left join ranked_key_result_check_in on user_with_first_access.user_id = ranked_key_result_check_in.user_id
      where rank = 1 or rank is null
  ),

  final as (
    select
      user_with_first_check_in.user_id,
      first_access_amplitude_event_id,
      first_key_result_check_in_id,
      ranked_key_result_comment.id as first_key_result_comment_id
      from user_with_first_check_in
      left join ranked_key_result_comment on user_with_first_check_in.user_id = ranked_key_result_comment.user_id
      where rank = 1 or rank is null
  )

select * from final
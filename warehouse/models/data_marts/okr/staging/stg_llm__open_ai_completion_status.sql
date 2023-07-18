with src_dim__llm_openai as (
    select
        *
    from
        {{ source('conformed', 'llm__openaicompletion') }}
),

final as (
    select
        SPLIT_PART(id, '.', 3) :: uuid as id,
        input,
        model :: text,
        action :: text,
        entity :: text,
        output :: text,
        status :: text,
        request,
        response,
        createdat :: timestamp as created_at,
        referenceid as reference_id,
        requestedat :: timestamp as requested_at,
        respondedat :: timestamp as responded_at,
        prompt_tokens,
        completion_tokens,
        totaltokens as total_tokens,
        requesteruserid as requester_user_id,
        requesterteamid as requester_team_id,
        requestercompanyid as requester_company_id
    from
        src_dim__llm_openai
)
select
    *
from
    final

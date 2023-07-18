with src_dim__llm_openai as (
    select
        *
    from
        {{ source('conformed', 'llm__openaicompletion') }}
),

final as (
    select
        SPLIT_PART(id, '.', 3) :: uuid as id,
        input :: jsonb,
        messages :: jsonb,
        model :: text,
        action :: text,
        entity :: text,
        output :: text,
        status :: text,
        request :: jsonb,
        response :: jsonb,
        createdat :: timestamp as created_at,
        referenceid :: uuid as reference_id,
        requestedat :: timestamp as requested_at,
        respondedat :: timestamp as responded_at,
        promptTokens :: number as prompt_tokens,
        completionTokens :: number as completion_tokens,
        estimatedCompletionTokens :: number as estimated_completion_tokens,
        estimatedPromptTokens :: number as estimated_prompt_tokens,
        totaltokens :: number as total_tokens,
        requesteruserid :: uuid as requester_user_id,
        requesterteamid :: uuid as requester_team_id,
        requestercompanyid :: uuid as requester_company_id
    from
        src_dim__llm_openai
)
select
    *
from
    final

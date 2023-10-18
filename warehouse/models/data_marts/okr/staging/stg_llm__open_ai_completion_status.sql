with src_dim__llm_openai as (
    select
        *
    from
        {{ source('llm', 'openaicompletion') }}
),

final as (
    select
        id :: text,
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
        referenceid :: text as reference_id,
        requestedat :: timestamp as requested_at,
        respondedat :: timestamp as responded_at,
        promptTokens :: integer as prompt_tokens,
        completionTokens :: integer as completion_tokens,
        estimatedCompletionTokens :: integer as estimated_completion_tokens,
        estimatedPromptTokens :: integer as estimated_prompt_tokens,
        totaltokens :: integer as total_tokens,
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

with src_dim__llm_openai as (
    select
        *
    from
        {{ source('llm', 'OpenAiCompletion') }}
),

final as (
    select
        id :: text,
        -- input :: jsonb,
        -- messages :: jsonb,
        model :: text,
        action :: text,
        entity :: text,
        output :: text,
        status :: text,
        -- request :: jsonb,
        -- response :: jsonb,
        "createdAt" :: timestamp as created_at,
        "referenceId" :: text as reference_id,
        "requestedAt" :: timestamp as requested_at,
        "respondedAt" :: timestamp as responded_at,
        "promptTokens" :: integer as prompt_tokens,
        "completionTokens" :: integer as completion_tokens,
        "estimatedCompletionTokens" :: integer as estimated_completion_tokens,
        "estimatedPromptTokens" :: integer as estimated_prompt_tokens,
        "totalTokens" :: integer as total_tokens,
        "requesterUserId" :: uuid as requester_user_id,
        "requesterTeamId" :: uuid as requester_team_id,
        "requesterCompanyId" :: uuid as requester_company_id
    from
        src_dim__llm_openai
)
select
    *
from
    final

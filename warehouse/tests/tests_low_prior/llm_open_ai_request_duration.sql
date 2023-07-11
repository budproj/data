select
    width_bucket(
        extract (
            epoch
            from
                responded_at - requested_at
        ),
        0,
        60,
        60
    ) as duration_in_seconds,
    count(*)
from
    {{ ref('stg_llm__open_ai_completion_status') }}
where
    requested_at is not null
    and responded_at is not null
group by
    duration_in_seconds
order by
    duration_in_seconds;

-- OpenAI total tokens usage histogram for analytical purposes. At first, does not require any alarms
select
    width_bucket(total_tokens, 0, 4000, 10) * 400 as total_tokens_bucket,
    count(*)
from
    {{ ref('stg_llm__open_ai_completion_status') }}
where
    total_tokens is not null
    and "model" = 'gpt-3.5-turbo'
group by
    total_tokens_bucket
order by
    total_tokens_bucket;

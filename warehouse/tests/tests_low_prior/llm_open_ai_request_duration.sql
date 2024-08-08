-- OpenAI request duration histogram for analytical purposes. At first, does not require any alarms
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
    duration_in_seconds

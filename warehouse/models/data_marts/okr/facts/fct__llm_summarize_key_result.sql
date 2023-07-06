with llm_kr_summarize as (
    select
        *
    from
        {{ ref('dim__llm_open_ai_completion') }}
    where
        entity = 'KeyResult'
        and action = 'Summarize'
),
final as (
    select
        llm_kr_summarize.*
    from
        llm_kr_summarize
)

select
    *
from
    final

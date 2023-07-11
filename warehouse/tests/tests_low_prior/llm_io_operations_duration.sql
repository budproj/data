with openai_stats as (
    select *,
           extract(epoch from (requested_at - created_at)) as db_write_duration,
           extract(epoch from (responded_at - requested_at)) as openai_duration
    from {{ ref('stg_llm__open_ai_completion_status') }}
)
select 
    action,
    entity,
    model,
    -- Database write duration percentiles
    (percentile_cont(0.50) within group (order by db_write_duration))::decimal(8, 2) as db_write_p50,
    (percentile_cont(0.95) within group (order by db_write_duration))::decimal(8, 2) as db_write_p95,
    (percentile_cont(0.99) within group (order by db_write_duration))::decimal(8, 2) as db_write_p99,
    -- OpenAI request duration percentiles
    (percentile_cont(0.50) within group (order by openai_duration))::decimal(8, 2) as openai_p50,
    (percentile_cont(0.95) within group (order by openai_duration))::decimal(8, 2) as openai_p95,
    (percentile_cont(0.99) within group (order by openai_duration))::decimal(8, 2) as openai_p99,
    -- OpenAI consumed tokens percentiles
    (percentile_cont(0.50) within group (order by consumed_tokens))::int as consumed_tokens_p50,
    (percentile_cont(0.95) within group (order by consumed_tokens))::int as consumed_tokens_p95,
    (percentile_cont(0.99) within group (order by consumed_tokens))::int as consumed_tokens_p99,
    -- OpenAI produced tokens percentiles
    (percentile_cont(0.50) within group (order by produced_tokens))::int as produced_tokens_p50,
    (percentile_cont(0.95) within group (order by produced_tokens))::int as produced_tokens_p95,
    (percentile_cont(0.99) within group (order by produced_tokens))::int as produced_tokens_p99
from 
    openai_stats
group by 
    action, 
    entity, 
    model

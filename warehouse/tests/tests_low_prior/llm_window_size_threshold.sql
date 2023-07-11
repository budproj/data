with context_window as (
    select
        *
    from
        (
            values
                ('gpt-3.5-turbo', 4000),
                ('gpt-3.5-turbo-16k', 16000),
                ('gpt-4', 8000),
                ('gpt-4-32k', 32000)
        ) as context_window("model", "window_size")
),
threshold as (
    select
        *
    from
        (
            values
                ('normal', 0, 0.50),
                ('look_out_50%', 0.50, 0.75),
                ('warning_75%', 0.75, 0.90),
                ('severe_90%', 0.90, 0.95),
                ('critical_95%', 0.95, 1),
                ('exceeded_100%', 1, float8 '+infinity')
        ) as threshold("label", "min", "max")
)
select
    oac.model as model,
    t.label as threshold,
    count(*) as "count"
from
    {{ ref('stg_llm__open_ai_completion_status') }} oac
    inner join context_window cw on cw.model = oac.model
    inner join threshold t on oac.total_tokens :: float / cw.window_size :: float between t.min
    and t.max - 0.00001
where
    oac.total_tokens is not null
    and oac.model = cw.model
    and t.label <> 'normal'
group by
    oac.model,
    t.label,
    t.min
order by
    t.min

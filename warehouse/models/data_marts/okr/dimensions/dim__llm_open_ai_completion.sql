with
  stg_llm__open_ai_completion_status as (
    select * from {{ ref('stg_llm__open_ai_completion_status') }}
  ),

  final as  (
    select
      stg_llm__open_ai_completion_status.*
      from stg_llm__open_ai_completion_status
  )

select * from final


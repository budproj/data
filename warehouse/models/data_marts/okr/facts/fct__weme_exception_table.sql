with time_responsavel as (
  SELECT
    distinct on (
      t.name,
      kr.title,
      o.title,
      krp.progress,
      krci.created_at
    ) t.name AS "Time responsavel",
    o.title AS "Objetivo",
    kr.title AS "Resultado-chave",
    case
      when krp.progress = 1 then 'Concluido'
      when krp.progress >= 0.7 then '> 70%'
      when krp.progress is not null then 'Em andamento'
      else 'Sem check-in ainda'
    end as "Status de Progresso",
    krp.progress as "Progresso",
    krci.created_at AS "Último check-in"
  FROM
    {{ ref('dim__key_result') }} kr
    LEFT JOIN {{ ref('fct__key_result_progress')}} krp ON kr.id = krp.key_result_id
    LEFT JOIN {{ ref('fct__company_members')}} cm ON kr.owner_id = cm.user_id
    LEFT JOIN {{ ref('dim__company')}} c ON cm.company_id = c.id
    LEFT JOIN {{ ref('dim__team')}} t ON kr.team_id = t.id
    LEFT JOIN {{ ref('dim__objective')}} o ON kr.objective_id = o.id
    LEFT JOIN {{ ref('dim__cycle')}} cy ON kr.cycle_id = cy.id
    LEFT JOIN {{ ref('fct__key_result_latest_check_in')}} krlci ON kr.id = krlci.key_result_id
    LEFT JOIN {{ ref('dim__key_result_check_in')}} krci ON krlci.key_result_check_in_id = krci.id
  WHERE
    (
      cy.active = TRUE
      AND c.name = 'weme'
    )
  ORDER BY
    krci.created_at DESC
)
select
  *
from
  time_responsavel
where
  "Time responsavel" is not null
order by
  1,
  2,
  3

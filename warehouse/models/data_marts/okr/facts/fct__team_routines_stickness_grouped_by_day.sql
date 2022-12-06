with base as (
    select
        *
    from
        {{ ref('fct__team_routines_stickness_by_day') }}
)
select
    day,
    sum(
        case
            when wau >= 0.5 then 1
            else 0
        end
    ) as teams_with_at_least_50_percent_wau,
    sum(
        case
            when mau >= 0.5 then 1
            else 0
        end
    ) as teams_with_at_least_50_percent_mau,
    sum(
        case
            when wau >= 0.5 then 1
            else 0
        end
    ) :: float / nullif(count(distinct team_id), 0) as wau,
    sum(
        case
            when mau >= 0.5 then 1
            else 0
        end
    ) :: float / nullif(count(distinct team_id), 0) as mau,
    sum(
        case
            when wau >= 0.5 then 1
            else 0
        end
    ) :: float / nullif(
        sum(
            case
                when mau >= 0.5 then 1
                else 0
            end
        ) :: float,
        0
    ) as stickness
from
    base
group by
    day

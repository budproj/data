with base as (
    select
        *
    from
        {{ ref('fct__wau_by_day') }}
),
first_week_each_company as (
    select
        company_id,
        min(day) as first_week
    from
        base
    where
        wau > 0
    group by
        company_id
),
companies as (
    select
        *
    from
        {{ ref('dim__company') }}
),
cohort as (
    select
        b.company_id,
        (
            EXTRACT(
                days
                FROM
                    (date_trunc('week', day) - f.first_week)
            ) / 7
        ) :: int as week,
        sum(unique_active_users) :: float / sum(unique_users) as average_wau
    from
        base b
        join first_week_each_company f on b.company_id = f.company_id
    group by
        b.company_id,
        (
            EXTRACT(
                days
                FROM
                    (date_trunc('week', day) - f.first_week)
            ) / 7
        ) :: int
)
select
    null :: uuid as company_id,
    'Geral' as company_name,
    avg(
        case
            when week = 0 then average_wau
        end
    ) as week0,
    avg(
        case
            when week = 1 then average_wau
        end
    ) as week1,
    avg(
        case
            when week = 2 then average_wau
        end
    ) as week2,
    avg(
        case
            when week = 3 then average_wau
        end
    ) as week3,
    avg(
        case
            when week = 4 then average_wau
        end
    ) as week4,
    avg(
        case
            when week = 5 then average_wau
        end
    ) as week5,
    avg(
        case
            when week = 6 then average_wau
        end
    ) as week6,
    avg(
        case
            when week = 7 then average_wau
        end
    ) as week7,
    avg(
        case
            when week = 8 then average_wau
        end
    ) as week8,
    avg(
        case
            when week = 9 then average_wau
        end
    ) as week9,
    avg(
        case
            when week = 10 then average_wau
        end
    ) as week10,
    avg(
        case
            when week = 11 then average_wau
        end
    ) as week11,
    avg(
        case
            when week = 12 then average_wau
        end
    ) as week12,
    avg(
        case
            when week = 13 then average_wau
        end
    ) as week13,
    avg(
        case
            when week = 14 then average_wau
        end
    ) as week14,
    avg(
        case
            when week = 15 then average_wau
        end
    ) as week15,
    avg(
        case
            when week = 16 then average_wau
        end
    ) as week16,
    avg(
        case
            when week = 17 then average_wau
        end
    ) as week17,
    avg(
        case
            when week = 18 then average_wau
        end
    ) as week18,
    avg(
        case
            when week = 19 then average_wau
        end
    ) as week19,
    avg(
        case
            when week = 20 then average_wau
        end
    ) as week20,
    avg(
        case
            when week = 21 then average_wau
        end
    ) as week21,
    avg(
        case
            when week = 22 then average_wau
        end
    ) as week22,
    avg(
        case
            when week = 23 then average_wau
        end
    ) as week23,
    avg(
        case
            when week = 24 then average_wau
        end
    ) as week24,
    avg(
        case
            when week = 25 then average_wau
        end
    ) as week25,
    avg(
        case
            when week = 26 then average_wau
        end
    ) as week26,
    avg(
        case
            when week = 27 then average_wau
        end
    ) as week27,
    avg(
        case
            when week = 28 then average_wau
        end
    ) as week28,
    avg(
        case
            when week = 29 then average_wau
        end
    ) as week29,
    avg(
        case
            when week = 30 then average_wau
        end
    ) as week30,
    avg(
        case
            when week = 31 then average_wau
        end
    ) as week31,
    avg(
        case
            when week = 32 then average_wau
        end
    ) as week32,
    avg(
        case
            when week = 33 then average_wau
        end
    ) as week33,
    avg(
        case
            when week = 34 then average_wau
        end
    ) as week34,
    avg(
        case
            when week = 35 then average_wau
        end
    ) as week35,
    avg(
        case
            when week = 36 then average_wau
        end
    ) as week36,
    avg(
        case
            when week = 37 then average_wau
        end
    ) as week37,
    avg(
        case
            when week = 38 then average_wau
        end
    ) as week38,
    avg(
        case
            when week = 39 then average_wau
        end
    ) as week39,
    avg(
        case
            when week = 40 then average_wau
        end
    ) as week40,
    avg(
        case
            when week = 41 then average_wau
        end
    ) as week41,
    avg(
        case
            when week = 42 then average_wau
        end
    ) as week42,
    avg(
        case
            when week = 43 then average_wau
        end
    ) as week43,
    avg(
        case
            when week = 44 then average_wau
        end
    ) as week44,
    avg(
        case
            when week = 45 then average_wau
        end
    ) as week45,
    avg(
        case
            when week = 46 then average_wau
        end
    ) as week46,
    avg(
        case
            when week = 47 then average_wau
        end
    ) as week47,
    avg(
        case
            when week = 48 then average_wau
        end
    ) as week48,
    avg(
        case
            when week = 49 then average_wau
        end
    ) as week49,
    avg(
        case
            when week = 50 then average_wau
        end
    ) as week50,
    avg(
        case
            when week = 51 then average_wau
        end
    ) as week51,
    avg(
        case
            when week = 52 then average_wau
        end
    ) as week52,
    avg(
        case
            when week = 53 then average_wau
        end
    ) as week53,
    avg(
        case
            when week = 54 then average_wau
        end
    ) as week54,
    avg(
        case
            when week = 55 then average_wau
        end
    ) as week55,
    avg(
        case
            when week = 56 then average_wau
        end
    ) as week56,
    avg(
        case
            when week = 57 then average_wau
        end
    ) as week57,
    avg(
        case
            when week = 58 then average_wau
        end
    ) as week58,
    avg(
        case
            when week = 59 then average_wau
        end
    ) as week59,
    avg(
        case
            when week = 60 then average_wau
        end
    ) as week60,
    avg(
        case
            when week = 61 then average_wau
        end
    ) as week61,
    avg(
        case
            when week = 62 then average_wau
        end
    ) as week62,
    avg(
        case
            when week = 63 then average_wau
        end
    ) as week63,
    avg(
        case
            when week = 64 then average_wau
        end
    ) as week64,
    avg(
        case
            when week = 65 then average_wau
        end
    ) as week65,
    avg(
        case
            when week = 66 then average_wau
        end
    ) as week66,
    avg(
        case
            when week = 67 then average_wau
        end
    ) as week67,
    avg(
        case
            when week = 68 then average_wau
        end
    ) as week68,
    avg(
        case
            when week = 69 then average_wau
        end
    ) as week69,
    avg(
        case
            when week = 70 then average_wau
        end
    ) as week70,
    avg(
        case
            when week = 71 then average_wau
        end
    ) as week71,
    avg(
        case
            when week = 72 then average_wau
        end
    ) as week72,
    avg(
        case
            when week = 73 then average_wau
        end
    ) as week73,
    avg(
        case
            when week = 74 then average_wau
        end
    ) as week74,
    avg(
        case
            when week = 75 then average_wau
        end
    ) as week75,
    avg(
        case
            when week = 76 then average_wau
        end
    ) as week76,
    avg(
        case
            when week = 77 then average_wau
        end
    ) as week77,
    avg(
        case
            when week = 78 then average_wau
        end
    ) as week78,
    avg(
        case
            when week = 79 then average_wau
        end
    ) as week79,
    avg(
        case
            when week = 80 then average_wau
        end
    ) as week80,
    avg(
        case
            when week = 81 then average_wau
        end
    ) as week81,
    avg(
        case
            when week = 82 then average_wau
        end
    ) as week82,
    avg(
        case
            when week = 83 then average_wau
        end
    ) as week83,
    avg(
        case
            when week = 84 then average_wau
        end
    ) as week84,
    avg(
        case
            when week = 85 then average_wau
        end
    ) as week85,
    avg(
        case
            when week = 86 then average_wau
        end
    ) as week86,
    avg(
        case
            when week = 87 then average_wau
        end
    ) as week87,
    avg(
        case
            when week = 88 then average_wau
        end
    ) as week88,
    avg(
        case
            when week = 89 then average_wau
        end
    ) as week89,
    avg(
        case
            when week = 90 then average_wau
        end
    ) as week90,
    avg(
        case
            when week = 91 then average_wau
        end
    ) as week91,
    avg(
        case
            when week = 92 then average_wau
        end
    ) as week92,
    avg(
        case
            when week = 93 then average_wau
        end
    ) as week93,
    avg(
        case
            when week = 94 then average_wau
        end
    ) as week94,
    avg(
        case
            when week = 95 then average_wau
        end
    ) as week95,
    avg(
        case
            when week = 96 then average_wau
        end
    ) as week96,
    avg(
        case
            when week = 97 then average_wau
        end
    ) as week97,
    avg(
        case
            when week = 98 then average_wau
        end
    ) as week98,
    avg(
        case
            when week = 99 then average_wau
        end
    ) as week99
from
    cohort
union
all (
    select
        company_id,
        c.name,
        sum(
            case
                when week = 0 then average_wau
                else 0
            end
        ) as week0,
        sum(
            case
                when week = 1 then average_wau
                else 0
            end
        ) as week1,
        sum(
            case
                when week = 2 then average_wau
                else 0
            end
        ) as week2,
        sum(
            case
                when week = 3 then average_wau
                else 0
            end
        ) as week3,
        sum(
            case
                when week = 4 then average_wau
                else 0
            end
        ) as week4,
        sum(
            case
                when week = 5 then average_wau
                else 0
            end
        ) as week5,
        sum(
            case
                when week = 6 then average_wau
                else 0
            end
        ) as week6,
        sum(
            case
                when week = 7 then average_wau
                else 0
            end
        ) as week7,
        sum(
            case
                when week = 8 then average_wau
                else 0
            end
        ) as week8,
        sum(
            case
                when week = 9 then average_wau
                else 0
            end
        ) as week9,
        sum(
            case
                when week = 10 then average_wau
                else 0
            end
        ) as week10,
        sum(
            case
                when week = 11 then average_wau
                else 0
            end
        ) as week11,
        sum(
            case
                when week = 12 then average_wau
                else 0
            end
        ) as week12,
        sum(
            case
                when week = 13 then average_wau
                else 0
            end
        ) as week13,
        sum(
            case
                when week = 14 then average_wau
                else 0
            end
        ) as week14,
        sum(
            case
                when week = 15 then average_wau
                else 0
            end
        ) as week15,
        sum(
            case
                when week = 16 then average_wau
                else 0
            end
        ) as week16,
        sum(
            case
                when week = 17 then average_wau
                else 0
            end
        ) as week17,
        sum(
            case
                when week = 18 then average_wau
                else 0
            end
        ) as week18,
        sum(
            case
                when week = 19 then average_wau
                else 0
            end
        ) as week19,
        sum(
            case
                when week = 20 then average_wau
                else 0
            end
        ) as week20,
        sum(
            case
                when week = 21 then average_wau
                else 0
            end
        ) as week21,
        sum(
            case
                when week = 22 then average_wau
                else 0
            end
        ) as week22,
        sum(
            case
                when week = 23 then average_wau
                else 0
            end
        ) as week23,
        sum(
            case
                when week = 24 then average_wau
                else 0
            end
        ) as week24,
        sum(
            case
                when week = 25 then average_wau
                else 0
            end
        ) as week25,
        sum(
            case
                when week = 26 then average_wau
                else 0
            end
        ) as week26,
        sum(
            case
                when week = 27 then average_wau
                else 0
            end
        ) as week27,
        sum(
            case
                when week = 28 then average_wau
                else 0
            end
        ) as week28,
        sum(
            case
                when week = 29 then average_wau
                else 0
            end
        ) as week29,
        sum(
            case
                when week = 30 then average_wau
                else 0
            end
        ) as week30,
        sum(
            case
                when week = 31 then average_wau
                else 0
            end
        ) as week31,
        sum(
            case
                when week = 32 then average_wau
                else 0
            end
        ) as week32,
        sum(
            case
                when week = 33 then average_wau
                else 0
            end
        ) as week33,
        sum(
            case
                when week = 34 then average_wau
                else 0
            end
        ) as week34,
        sum(
            case
                when week = 35 then average_wau
                else 0
            end
        ) as week35,
        sum(
            case
                when week = 36 then average_wau
                else 0
            end
        ) as week36,
        sum(
            case
                when week = 37 then average_wau
                else 0
            end
        ) as week37,
        sum(
            case
                when week = 38 then average_wau
                else 0
            end
        ) as week38,
        sum(
            case
                when week = 39 then average_wau
                else 0
            end
        ) as week39,
        sum(
            case
                when week = 40 then average_wau
                else 0
            end
        ) as week40,
        sum(
            case
                when week = 41 then average_wau
                else 0
            end
        ) as week41,
        sum(
            case
                when week = 42 then average_wau
                else 0
            end
        ) as week42,
        sum(
            case
                when week = 43 then average_wau
                else 0
            end
        ) as week43,
        sum(
            case
                when week = 44 then average_wau
                else 0
            end
        ) as week44,
        sum(
            case
                when week = 45 then average_wau
                else 0
            end
        ) as week45,
        sum(
            case
                when week = 46 then average_wau
                else 0
            end
        ) as week46,
        sum(
            case
                when week = 47 then average_wau
                else 0
            end
        ) as week47,
        sum(
            case
                when week = 48 then average_wau
                else 0
            end
        ) as week48,
        sum(
            case
                when week = 49 then average_wau
                else 0
            end
        ) as week49,
        sum(
            case
                when week = 50 then average_wau
                else 0
            end
        ) as week50,
        sum(
            case
                when week = 51 then average_wau
                else 0
            end
        ) as week51,
        sum(
            case
                when week = 52 then average_wau
                else 0
            end
        ) as week52,
        sum(
            case
                when week = 53 then average_wau
                else 0
            end
        ) as week53,
        sum(
            case
                when week = 54 then average_wau
                else 0
            end
        ) as week54,
        sum(
            case
                when week = 55 then average_wau
                else 0
            end
        ) as week55,
        sum(
            case
                when week = 56 then average_wau
                else 0
            end
        ) as week56,
        sum(
            case
                when week = 57 then average_wau
                else 0
            end
        ) as week57,
        sum(
            case
                when week = 58 then average_wau
                else 0
            end
        ) as week58,
        sum(
            case
                when week = 59 then average_wau
                else 0
            end
        ) as week59,
        sum(
            case
                when week = 60 then average_wau
                else 0
            end
        ) as week60,
        sum(
            case
                when week = 61 then average_wau
                else 0
            end
        ) as week61,
        sum(
            case
                when week = 62 then average_wau
                else 0
            end
        ) as week62,
        sum(
            case
                when week = 63 then average_wau
                else 0
            end
        ) as week63,
        sum(
            case
                when week = 64 then average_wau
                else 0
            end
        ) as week64,
        sum(
            case
                when week = 65 then average_wau
                else 0
            end
        ) as week65,
        sum(
            case
                when week = 66 then average_wau
                else 0
            end
        ) as week66,
        sum(
            case
                when week = 67 then average_wau
                else 0
            end
        ) as week67,
        sum(
            case
                when week = 68 then average_wau
                else 0
            end
        ) as week68,
        sum(
            case
                when week = 69 then average_wau
                else 0
            end
        ) as week69,
        sum(
            case
                when week = 70 then average_wau
                else 0
            end
        ) as week70,
        sum(
            case
                when week = 71 then average_wau
                else 0
            end
        ) as week71,
        sum(
            case
                when week = 72 then average_wau
                else 0
            end
        ) as week72,
        sum(
            case
                when week = 73 then average_wau
                else 0
            end
        ) as week73,
        sum(
            case
                when week = 74 then average_wau
                else 0
            end
        ) as week74,
        sum(
            case
                when week = 75 then average_wau
                else 0
            end
        ) as week75,
        sum(
            case
                when week = 76 then average_wau
                else 0
            end
        ) as week76,
        sum(
            case
                when week = 77 then average_wau
                else 0
            end
        ) as week77,
        sum(
            case
                when week = 78 then average_wau
                else 0
            end
        ) as week78,
        sum(
            case
                when week = 79 then average_wau
                else 0
            end
        ) as week79,
        sum(
            case
                when week = 80 then average_wau
                else 0
            end
        ) as week80,
        sum(
            case
                when week = 81 then average_wau
                else 0
            end
        ) as week81,
        sum(
            case
                when week = 82 then average_wau
                else 0
            end
        ) as week82,
        sum(
            case
                when week = 83 then average_wau
                else 0
            end
        ) as week83,
        sum(
            case
                when week = 84 then average_wau
                else 0
            end
        ) as week84,
        sum(
            case
                when week = 85 then average_wau
                else 0
            end
        ) as week85,
        sum(
            case
                when week = 86 then average_wau
                else 0
            end
        ) as week86,
        sum(
            case
                when week = 87 then average_wau
                else 0
            end
        ) as week87,
        sum(
            case
                when week = 88 then average_wau
                else 0
            end
        ) as week88,
        sum(
            case
                when week = 89 then average_wau
                else 0
            end
        ) as week89,
        sum(
            case
                when week = 90 then average_wau
                else 0
            end
        ) as week90,
        sum(
            case
                when week = 91 then average_wau
                else 0
            end
        ) as week91,
        sum(
            case
                when week = 92 then average_wau
                else 0
            end
        ) as week92,
        sum(
            case
                when week = 93 then average_wau
                else 0
            end
        ) as week93,
        sum(
            case
                when week = 94 then average_wau
                else 0
            end
        ) as week94,
        sum(
            case
                when week = 95 then average_wau
                else 0
            end
        ) as week95,
        sum(
            case
                when week = 96 then average_wau
                else 0
            end
        ) as week96,
        sum(
            case
                when week = 97 then average_wau
                else 0
            end
        ) as week97,
        sum(
            case
                when week = 98 then average_wau
                else 0
            end
        ) as week98,
        sum(
            case
                when week = 99 then average_wau
                else 0
            end
        ) as week99
    from
        cohort
        left join companies c on cohort.company_id = c.id
    group by
        company_id,
        c.name
    order by
        c.name
)

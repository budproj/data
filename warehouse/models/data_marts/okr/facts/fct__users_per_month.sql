with
  users as (
    select * from {{ ref('dim__user') }}
  ),

  final as (
    select
      date_trunc('month', created_at) as month,
      sum(count(*)) over (order by date_trunc('month', created_at)) as amount_of_users
    from users
    where status = 'ACTIVE'
    group by date_trunc('month', created_at)
  )

select * from final

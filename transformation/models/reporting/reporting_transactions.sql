


with transactions as (
    select * from {{ref('transactions_euro_daily')}}
)
select * from transactions
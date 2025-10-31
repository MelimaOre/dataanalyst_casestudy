
with report as (
    select * from {{ref('stg_raw_staging__accounts')}}
)
select * from report

-- Implement a dbt model that creates a materialized table in the reporting schema
-- The model should sum up all transactions in EUR (Euro) per customer, account, branch and date
-- Use the provided exchange rate table for currency conversion across all dates
-- Include appropriate dbt tests for data quality validation


---> reporting for dashboard for the Branch Performance Manager


-- customers: Customer demographics and branch assignments
-- accounts: Account information linked to customers
-- transactions: All transaction data with multi-currency support
-- loans: Loan portfolio with approval/rejection status
-- fx_rates: Exchange rates for currency conversion
-- currencies: Currency lookup table


-- 1 Euro = fx_rate currency_iso_code --> amount_euro = amount / fx_rate



with 

------------------------
---Input
-------------------------


fx_rate_to_euro as  ---just data for 1.3.2023 !
(
  select
    upper(currency_iso_code) as currency_iso_code,
    cast(nullif(replace(trim(fx_rate), ',', '.'), '') AS numeric) as rate_to_euro, 
    cast(fx_rate_date as date) as rate_date -- ? transactions for jan. but fx_rate just for 1.3 .2023
  from {{ref('stg_raw_staging__fx_rates')}} 
  --from staging.stg_raw_staging__fx_rates
),


transactions as (
  select
    cast(transaction_id as bigint) as transaction_id,
    cast(account_id as bigint) as account_id,
    transaction_type,
    to_date(trim(transaction_date), 'DD.MM.YYYY') as transaction_date,
    cast(nullif(replace(trim(transaction_amount), ',', '.'), '') as numeric) as transaction_amount,
    upper(transaction_currency) as transaction_currency --- ? One RON1 mistake? missing in fx_rates
  from {{ref('stg_raw_staging__transactions')}} 
  --from staging.stg_raw_staging__transactions
   where transaction_currency in  (select currency_iso_code as transaction_currency from fx_rate_to_euro )
),


customers as (
  select
    cast(customer_id as bigint) as customer_id,
    cast(age as integer) as age
  from {{ref('stg_raw_staging__customers')}} 
  --from staging.stg_raw_staging__customers
),


accounts as (
  select
    cast(account_id as bigint) as account_id,
    cast(customer_id as bigint) as customer_id,
    account_type
  from {{ref('stg_raw_staging__accounts')}} 
  --from staging.stg_raw_staging__accounts
),


------------------------
--- Transactions
-------------------------


transactions_to_euro as
(
select
    t.transaction_id,
    t.account_id,
    t.transaction_date,
    t.transaction_type,
    t.transaction_amount as amount,
    t.transaction_currency,    
    fx.rate_to_euro,
case
  when t.transaction_currency = 'EUR' then t.transaction_amount
      else t.transaction_amount / fx.rate_to_euro
end as amount_euro
from transactions t
left join fx_rate_to_euro fx
  on fx.currency_iso_code = t.transaction_currency
  and  fx.rate_date = DATE '2023-03-01'
-- and fx.rate_date = t.transaction_date -- ? importand join but not all data in fx_rate 
 ),
 
 customers_accounts as 
 ( 
 select 
 	c.customer_id,
 	c.age,
 	a.account_id,
	a.account_type
 from customers c 
 join accounts a on c.customer_id = a.customer_id
 ),
 
 
 trans_per_customer_account as 
 (
 select 
 	ca.customer_id,
 	ca.age,
	ca.account_id,
 	ca.account_type,
 	tfxe.transaction_id,
    tfxe.transaction_date,
    tfxe.transaction_type,
    tfxe.transaction_currency,
 	tfxe.amount,
 	tfxe.amount_euro
 from transactions_to_euro tfxe
 join  customers_accounts ca on tfxe.account_id = ca.account_id
 ),
 
  
  trans_daily as 
 (
select 
	transaction_date as date,	-- ? join with dim_date
	customer_id,
	account_id, 
	account_type, 
	transaction_type, 
	transaction_currency,
	sum(amount) as amount_original,
	--sum(case when t.transaction_currency = 'EUR' then amount else 0 end) as amount_original,
	sum(amount_euro) as amount_euro,
	Count(distinct transaction_id) as transaction_nr
from trans_per_customer_account
group by 
	transaction_date,
	customer_id,
	account_id, 
	account_type, 
	transaction_type, 
	transaction_currency
 ),
 

 
------------------------
--- Final
-------------------------
 
  
final as 
 (
select 
	date,	
	customer_id,
	account_id, 
	account_type, 
	transaction_type, 
	transaction_currency,
	amount_original,
	amount_euro,
	transaction_nr
from trans_daily
 )
 
 
 select * from final
With source as (
 select * from {{ source('erp_prod', 'bank_accounts') }}
)
select 


id as bank_account_id,
account_id,
company_id,


name,
branch_name,

ifsc_code,
swift_code,
currency,
country,
city,
created_at,
updated_at,
created_by,
state,
iban_number,

current_timestamp() as ingestion_timestamp,




from source 
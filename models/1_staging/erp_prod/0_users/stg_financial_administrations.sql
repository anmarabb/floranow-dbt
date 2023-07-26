With source as (
 select * from {{ source('erp_prod', 'financial_administrations') }}
)
select 

id,
name,
prefix,
created_at,
updated_at,
start_invoice_number,
current_invoice_number,
invoice_prefix,
credit_note_prefix,
payment_transaction_prefix,


current_timestamp() as ingestion_timestamp,




from source 
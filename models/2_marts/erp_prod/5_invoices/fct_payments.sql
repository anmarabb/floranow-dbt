with

source as ( 

select
master_date,
cridet_date,
payment_received_at,


Customer,
debtor_number,
account_manager,
user_category,
company_name,
Warehouse,
financial_administration,
payment_method,


payment_transaction_number,
credit_note_number,
invoice_number,


payment_amount,


current_timestamp() as insertion_timestamp,


from {{ref('int_payments')}} as py
)

select * from source


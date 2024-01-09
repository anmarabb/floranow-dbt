
--query for unreconciled payment transactions amount


select
case when cmi.date is not null then cmi.date else cmi.date end as master_date,
date(cmi.date)  as cridet_date,
warehouse,
payment_received_at,
Customer,
debtor_number,
user_category,
payment_method,
account_manager,
doc_type,
payment_transaction_number,
credit_note_number,
financial_administration,
company_name,


from {{ref('fct_move_items')}} as cmi 
where cmi.entry_type='CREDIT'
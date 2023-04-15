--create or replace view `floranow.Floranow_ERP.payments` as 

select 
pt.created_by,
pt.payment_method,

ba.name as bank_name,

i.number as invoice_number,
i2.number as Credit_note ,
concat( "https://erp.floranow.com/payment_transactions/", py.payment_transaction_id) as Payment_transaction,

py.created_at as payment_date,

py.total_amount,
py.paid_amount,

py.credit_note_amount,
py.updated_at,
py.id,
py.invoice_id,
py.credit_note_id,
py.payment_type, ----over_payed, credit, write_off, cheque, payment_by_credit, bank_transfer, visa_card, cash, null
py.currency,
py.added_by,
py.approved_by,
pt.collected_by,
pt.number,
pt.approval_code,


py.payment_transaction_id,
py.deleted_at,
py.netsuite_ref_id,
py.netsuite_failure_reason,



stg_users.city,
stg_users.customer,
stg_users.client_category,
stg_users.customer_type,
stg_users.payment_term,
stg_users.account_manager,
stg_users.country,
stg_users.financial_administration,
stg_users.debtor_number,
i.printed_at,

pt.payment_received_at,

from `floranow.erp_prod.payments` as py
left join `floranow.erp_prod.invoices` as i on py.invoice_id = i.id
left join `floranow.Floranow_ERP.users` as stg_users on stg_users.id = i.customer_id

left join `floranow.erp_prod.payment_transactions` as pt on py.payment_transaction_id = pt.id

left join `floranow.erp_prod.invoices` as i2 on py.credit_note_id = i2.id
left join `floranow.erp_prod.bank_accounts` as ba on stg_users.bank_account_id = ba.id



--where i.number = 'F10122712'

With source as (
 select * from {{ source('erp_prod', 'payment_terms') }}
)
select 


id,
name,
without_invoicing, --false, false
due_pattern, -- INSTANTLY, DUE_AFTER, null
items_collection_method, --1, 0

block_after,  --0 0 1 0, 0 0 0 0
unblock_amount, -- 0, 100

--template_id
    invoice_template_id,
    credit_note_template_id,
    statement_template_id,
    payment_receipt_template_id,
    creditable_invoice_template_id,
    ledger_template_id,

--ar_id
    invoice_template_ar_id,
    credit_note_template_ar_id,
    statement_template_ar_id,
    payment_receipt_template_ar_id,
    creditable_invoice_template_ar_id,
    ledger_template_ar_id,

--date
    created_at,
    updated_at,





statement_type,
invoicing_method,
payment_condition,


send_email,
auto_process,
tax_rate,

invoicing_master_user,
due_options,
description,
with_stamp,


block_after_interval,



current_timestamp() as ingestion_timestamp,




from source 
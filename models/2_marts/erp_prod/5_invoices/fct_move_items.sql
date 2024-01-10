with

source as ( 

select

credit_note_number,
payment_transaction_number,
approval_code,
CN_amount,
PT_amount,
user_id,

due_date,
date,
date(payment_received_at) as payment_received_at,

balance, --
residual, --
source_system,
reconciled,
documentable_type, --Invoice, PaymentTransaction, null
entry_type,  --CREDIT, DEBIT
currency,


--move_items
    company_name,
    financial_administration,

    payment_method,
    doc_number,
    doc_type,

    total_credits,
    total_debits,
    payments,
    other_credit,
    unreconciled_credits,
    unreconciled_debits,


    up_to_30_days,
    between_31_to_60_days,
    between_61_to_90_days,
    between_91_to_120_days,
    more_than_120_days,


    collectible_amount,

    gross_revenue,
    invoice_total_tax,
    credit_note_total_tax,
    credit_nots_with_tax,
    gross_revenue_with_tax,
    total_tax,
    credit_note,

    

    



--customer
    Customer,
    warehouse,
    debtor_number,
    user_category,
    payment_term,
    account_manager,
    credit_limit,

documentable_id,

current_timestamp() as insertion_timestamp 


from {{ref('int_move_items')}} as mi
)

select * from source


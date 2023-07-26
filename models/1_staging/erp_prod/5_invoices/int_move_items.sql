with

source as ( 
        
select     

mi.* EXCEPT(created_at),

--date
    case when mi.date is not null then mi.date else mi.created_at end as created_at, 
    case when pt.payment_received_at is not null then pt.payment_received_at else mi.created_at end as received_at, 




customer.name as Customer,
customer.account_manager,
customer.debtor_number,
customer.company_name,
customer.city,
customer.user_category,


--fct
    -mi.balance as paid_amount,
    -(mi.balance - mi.residual) as reconciled_amount,
    -mi.residual as un_reconciled_amount,

    current_timestamp() as insertion_timestamp, 

from {{ ref('stg_move_items')}} as mi
left join {{ ref('base_users') }} as customer on customer.id = mi.user_id
left join {{ ref('stg_payment_transactions') }} as pt on pt.payment_transaction_id = mi.documentable_id and mi.documentable_type = 'PaymentTransaction' and  mi.entry_type = 'CREDIT'
left join {{ source('erp_prod', 'financial_administrations') }} as fn on fn.id = pt.financial_administration_id
left join {{ source('erp_prod', 'bank_accounts') }} as ba on pt.bank_account_id = ba.id

left join {{ref('stg_invoices')}} as i on mi.documentable_id = i.invoice_header_id and mi.documentable_type = 'Invoice' and mi.entry_type = 'DEBIT'
left join {{ref('stg_invoices')}} as cn on mi.documentable_id = cn.invoice_header_id and mi.documentable_type = 'Invoice' and mi.entry_type = 'CREDIT'




    )

select * from source
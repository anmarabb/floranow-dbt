with

source as ( 
        
select     

moi.* EXCEPT(created_at),

--date
    case when moi.date is not null then moi.date else moi.created_at end as created_at, 
    case when pt.payment_received_at is not null then pt.payment_received_at else moi.created_at end as received_at, 




customer.name as Customer,
customer.account_manager,
customer.debtor_number,
customer.company_name,
customer.city,
customer.user_category,


--fct
    -moi.balance as paid_amount,
    -(moi.balance - moi.residual) as reconciled_amount,
    -moi.residual as un_reconciled_amount,

    current_timestamp() as insertion_timestamp, 

from {{ ref('stg_move_items')}} as moi
left join {{ ref('base_users') }} as customer on customer.id = moi.user_id
left join {{ ref('stg_payment_transactions') }} as pt on pt.payment_transaction_id = moi.documentable_id and moi.documentable_type = 'PaymentTransaction'
left join {{ source('erp_prod', 'financial_administrations') }} as fn on fn.id = pt.financial_administration_id
left join {{ source('erp_prod', 'bank_accounts') }} as ba on pt.bank_account_id = ba.id

where documentable_type = 'PaymentTransaction' and entry_type = 'CREDIT'




    )

select * from source
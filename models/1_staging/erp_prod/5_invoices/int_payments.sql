with

source as ( 
        
select     

py.*,


customer.name as Customer,
customer.debtor_number,
customer.account_manager,
customer.user_category,
customer.company_name,
customer.Warehouse,

fn.name as financial_administration,
pt.payment_method,


case when (pt.payment_transaction_id is null and cn.invoice_header_id is null and cmi.source_system = 'ODOO')   then 'ODOO' else pt.number end as payment_transaction_number,
case when (pt.payment_transaction_id is null and cn.invoice_header_id is null and cmi.source_system = 'ODOO')   then 'ODOO' else cn.invoice_number end as credit_note_number,
case when i.invoice_header_id is null and dmi.source_system = 'ODOO'   then 'ODOO' else i.invoice_number end as invoice_number,

pt.approval_code,


--date
    case when pt.payment_received_at is not null then pt.payment_received_at else cmi.date end as master_date,
    date(cmi.date) as cridet_date, -- date of paymnet_transaction
    date(pt.payment_received_at) as payment_received_at,



    py.total_amount as payment_amount,





current_timestamp() as insertion_timestamp, 

from {{ ref('stg_payments')}} as py

left join {{ ref('base_users') }} as customer on customer.id = py.user_id

left join {{ ref('stg_move_items') }} as dmi on py.debit_move_item_id = dmi.move_item_id
left join {{ ref('stg_move_items') }} as cmi on py.credit_move_item_id = cmi.move_item_id


left join {{ ref('stg_payment_transactions') }} as pt on pt.payment_transaction_id = cmi.documentable_id and cmi.documentable_type = 'PaymentTransaction' and  cmi.entry_type = 'CREDIT'

left join {{ref('stg_invoices')}} as i on dmi.documentable_id = i.invoice_header_id and dmi.documentable_type = 'Invoice' and dmi.entry_type = 'DEBIT'
left join {{ref('stg_invoices')}} as cn on cmi.documentable_id = cn.invoice_header_id and cmi.documentable_type = 'Invoice' and cmi.entry_type = 'CREDIT'

left join {{ref('stg_financial_administrations')}} as fn on fn.id = dmi.financial_administration_id 
--left join {{ref('stg_financial_administrations')}} as fn on fn.id = pt.financial_administration_id --pt.fn


--left join {{ref('stg_financial_administrations')}} as fn on fn.id = customer.financial_administration_id

--left join {{ ref('stg_warehouses') }} as w on w.warehouse_id = customer.warehouse_id 


left join {{ ref('stg_bank_accounts') }} as ba on customer.bank_account_id = ba.bank_account_id







    )

select * from source
with

source as ( 
        
select     

*,

current_timestamp() as insertion_timestamp, 

from {{ ref('stg_payments')}} as py

left join {{ ref('base_users') }} as customer on customer.id = py.user_id

left join {{ ref('stg_move_items') }} as dmi on py.debit_move_item_id = dmi.move_item_id
left join {{ ref('stg_move_items') }} as cmi on py.credit_move_item_id = cmi.move_item_id


left join {{ ref('stg_payment_transactions') }} as pt on pt.payment_transaction_id = cmi.documentable_id and cmi.documentable_type = 'PaymentTransaction' and  cmi.entry_type = 'CREDIT'

left join {{ref('stg_invoices')}} as i on dmi.documentable_id = i.invoice_header_id and dmi.documentable_type = 'Invoice' and dmi.entry_type = 'DEBIT'
left join {{ref('stg_invoices')}} as cn on cmi.documentable_id = cn.invoice_header_id and cmi.documentable_type = 'Invoice' and cmi.entry_type = 'CREDIT'

left join {{ref('stg_financial_administrations')}} as fn on fn.id = pt.financial_administration_id
--left join {{ref('stg_financial_administrations')}} as fn on fn.id = customer.financial_administration_id

left join {{ ref('stg_warehouses') }} as w on w.warehouse_id = customer.warehouse_id 


left join {{ ref('stg_bank_accounts') }} as ba on customer.bank_account_id = ba.bank_account_id







    )

select * from source
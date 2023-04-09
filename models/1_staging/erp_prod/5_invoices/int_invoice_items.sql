with

source as ( 
        
select     


i.financial_administration,


li.Supplier,


customer.name as Customer,
approved_by_id.name as approved_by,

ii.*,

current_timestamp() as insertion_timestamp, 

from {{ ref('stg_invoice_items') }} as ii
left join {{ ref('stg_invoices') }} as i on ii.invoice_id = i.invoice_id

left join {{ ref('base_users') }} as customer on customer.id = ii.customer_id
left join {{ref('base_users')}} as approved_by_id on approved_by_id.id = ii.approved_by_id

left join {{ ref('fct_order_items') }} as li on ii.line_item_id = li.line_item_id



    )

select * from source
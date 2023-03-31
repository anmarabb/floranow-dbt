with

source as ( 
        
select     

ii.*,

    current_timestamp() as insertion_timestamp, 

from {{ ref('stg_invoice_items') }} as ii
left join {{ ref('stg_invoices') }} as i on ii.invoice_id = i.id

left join {{ ref('base_users') }} as customer on customer.id = ii.customer_id

left join {{ ref('dim_line_items') }} as li on ii.line_item_id = li.line_item_id



    )

select * from source
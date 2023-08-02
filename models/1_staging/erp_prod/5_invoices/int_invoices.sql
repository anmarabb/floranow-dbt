with invoice_items as (

SELECT
ii.invoice_header_id,
sum(ii.quantity * li.unit_landed_cost)  as total_cost,


from {{ ref('stg_invoice_items') }} as ii 
left join {{ ref('fct_order_items') }} as li on ii.line_item_id = li.line_item_id

where ii.invoice_item_status = 'APPROVED' and ii.deleted_at is null
group by ii.invoice_header_id

),

prep_payments as (

    select 
py.invoice_header_id,

--sum(py.total_amount) as total_amount ,
--sum(py.paid_amount) as paid_amount,
sum(py.credit_note_amount) as credit_note_amount_used,


from {{ ref('int_payments') }} as py

group by invoice_header_id
)
        
select     

i.*,



concat(customer.debtor_number,i.items_collection_date) as drop_id, 



    printed_by.name as printed_by,

    customer.account_manager,
    customer.City,
    customer.user_category as client_category,
    customer.name as Customer,
    customer.Warehouse,


    ii.total_cost,
   -- prep_payments.paid_amount,
    prep_payments.credit_note_amount_used,






    current_timestamp() as insertion_timestamp, 

from {{ ref('stg_invoices')}} as i
left join {{ ref('base_users') }} as printed_by on printed_by.id = i.printed_by_id
left join {{ ref('base_users') }} as customer on customer.id = i.customer_id
left join invoice_items as ii on ii.invoice_header_id = i.invoice_header_id
left join prep_payments as prep_payments on prep_payments.invoice_header_id = i.invoice_header_id

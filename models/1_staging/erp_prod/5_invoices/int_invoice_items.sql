with

source as ( 
        
select     

ii.*EXCEPT(generation_type,invoice_type),

    --
        i.financial_administration,
        i.proforma_at,
        i.printed_at,
        i.invoice_type,
        i.generation_type,
        i.record_type,

        customer.name as Customer,
        customer.customer_type,
        
        li.Supplier,
        li.fulfillment_mode,
        li.order_status,
        li.record_type_details,
        li.ordered_quantity,
        li.fulfilled_quantity,



        approved_by_id.name as approved_by,
        
case when i.invoice_type = 'credit note' then -ii.quantity else ii.quantity end as invoiced_quantity,



i.proof_of_delivery_id as proof_of_delivery_id_inv,
li.proof_of_delivery_id as proof_of_delivery_id_line,


current_timestamp() as insertion_timestamp, 

from {{ ref('stg_invoice_items') }} as ii
left join {{ ref('stg_invoices') }} as i on ii.invoice_id = i.invoice_id

left join {{ ref('base_users') }} as customer on customer.id = ii.customer_id
left join {{ref('base_users')}} as approved_by_id on approved_by_id.id = ii.approved_by_id

left join {{ ref('fct_order_items') }} as li on ii.line_item_id = li.line_item_id

left join {{ ref('stg_proof_of_deliveries') }} as pod on li.proof_of_delivery_id = pod.proof_of_delivery_id


    )

select * from source



--where invoice_type = 'credit note' and creditable_id is null
--in the level of invoice_item all the credit note related to creditable_id (where invoice_type = 'credit note' and creditable_id is null)
with

source as ( 
        
select     

--Invoice Items

        ii.*,

ii.quantity * li.unit_landed_cost as total_cost,

        approved_by_id.name as approved_by,


        customer.name as Customer,
        customer.customer_type,
        customer.user_category,
        

--case when i.invoice_header_type = 'credit note' then -ii.quantity else ii.quantity end as invoiced_quantity,



--invoice Header

        i.financial_administration,
        i.invoice_header_created_at,
        i.invoice_header_printed_at,
        i.invoice_header_type,
        i.invoice_header_status,
        i.generation_type,
        i.record_type,
        i.proof_of_delivery_id as proof_of_delivery_id_inv,

        

--Line Items

        li.Supplier,
        li.supplier_id,
        li.Origin,
        li.fulfillment_mode,
        li.order_status,
        li.record_type_details,


        li.unit_landed_cost,



        li.order_number,



        
case 
when li.Supplier  = 'ASTRA Farms' then 'Astra'
when ii.meta_supplier_name in ('Astra Farm','Astra farm Barcode','Astra Farm - Event','Astra Flash Sale - R','Astra Flash sale - W') then 'Astra'
when li.feed_source_name in ('Express Jeddah','Express Dammam', 'Express Riyadh') and  li.parent_supplier in ('Holex','Floradelight', 'Waridi', 'Sierra','Vianen','PJ Dave Roses','Heritage Flowers','Décor Foliage','Sian Flowers', 'Flora Ola') then 'Non Astra'
when li.feed_source_name in ('Express Jeddah','Express Dammam', 'Express Riyadh', 'Express Tabuk') or li.Supplier in ('Express Jeddah','Express Dammam', 'Express Riyadh', 'Express Tabuk') then 'Astra'
else 'Non Astra'
end as sales_source,




li.proof_of_delivery_id as proof_of_delivery_id_line,


current_timestamp() as insertion_timestamp, 

from {{ ref('stg_invoice_items') }} as ii
left join {{ ref('stg_invoices') }} as i on ii.invoice_header_id = i.invoice_header_id

left join {{ ref('base_users') }} as customer on customer.id = ii.customer_id
left join {{ref('base_users')}} as approved_by_id on approved_by_id.id = ii.approved_by_id

left join {{ ref('fct_order_items') }} as li on ii.line_item_id = li.line_item_id

left join {{ ref('stg_proof_of_deliveries') }} as pod on li.proof_of_delivery_id = pod.proof_of_delivery_id




    )

select * from source


--where invoice_type = 'credit note' and creditable_id is null
--in the level of invoice_item all the credit note related to creditable_id (where invoice_type = 'credit note' and creditable_id is null)
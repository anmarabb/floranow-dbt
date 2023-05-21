with

source as ( 
        
select     

--Invoice Items

        ii.*,
        approved_by_id.name as approved_by,
        customer.name as Customer,
        customer.customer_type,
        customer.user_category,

        

            --calculating the sum of price_without_tax for invoice items printed in the last month and the corresponding month in the previous year
            --last month Sales Vs. corresponding month in the previous year
            -- M-1 Vs. M-1 (Last Y)
                case when date_diff(current_date(),date(i.invoice_header_printed_at), MONTH) = 1 then ii.price_without_tax else 0 end as m_1_sales,
                case when date_diff(current_date(),date(i.invoice_header_printed_at), YEAR) = 1 and extract(MONTH from date(i.invoice_header_printed_at)) = extract(MONTH from current_date()) - 1 then ii.price_without_tax else 0 end as m_1_sales_last_year,




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
        li.fulfillment_mode,
        li.order_status,
        li.record_type_details,
        li.ordered_quantity,
        li.fulfilled_quantity,



        
        
case when i.invoice_header_type = 'credit note' then -ii.quantity else ii.quantity end as invoiced_quantity,




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
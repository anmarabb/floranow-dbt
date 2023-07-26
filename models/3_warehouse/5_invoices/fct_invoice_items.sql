with

source as ( 

select

--invoice Items

creditable_id,
    
    --fct
        price_without_tax,
        price,
        total_tax,
        quantity,


    --dim
        financial_administration, -- Market
        Customer,
        user_category, -- Segment




        invoice_item_id,
        
        source_type, --ERP, Florisft
        invoice_item_type,
        invoice_item_status,
        customer_type,
        product_name as Product,
        
     --date
        order_date,
        delivery_date,
        deleted_at,




--Line Items

    --dim
        line_item_id,
        Supplier,
        supplier_id,
        Origin,
        fulfillment_mode,
        order_status,
        order_number,



--invoice Header

    --dim
        invoice_header_id,
        invoice_header_status, --draft, open, printed, signed, closed, canceled, rejected, voided
        invoice_header_type, --credit note, invoice
        generation_type,
        record_type,
        record_type_details,


    --date
        invoice_header_created_at,
        invoice_header_printed_at,



current_timestamp() as insertion_timestamp, 


from {{ref('int_invoice_items')}} as ii 
)

select * from source

--where invoice_type != 'credit note' and generation_type !='MANUAL'

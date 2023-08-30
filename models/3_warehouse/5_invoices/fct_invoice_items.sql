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
        total_cost,

        unit_price,
        unit_landed_cost,


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

sales_source,

CASE
    WHEN sales_source = 'Astra' and LOWER(Customer) LIKE '%tamimi%' THEN 'Astra - Tamimi Sales'
    WHEN sales_source = 'Non Astra' and LOWER(Customer) LIKE '%tamimi%' THEN 'Non Astra - Tamimi Sales'
    WHEN sales_source = 'Astra' and Customer IN ('REMA1','REMA2','REMA3','REMA4','REMA5','REMA6','REMA7','REMA8') THEN 'Astra - REMA Sales'
    WHEN sales_source = 'Non Astra' and Customer IN ('REMA1','REMA2','REMA3','REMA4','REMA5','REMA6','REMA7','REMA8') THEN 'Non Astra - REMA Sales'
    WHEN sales_source = 'Astra' then 'Astra'
    WHEN sales_source = 'Non Astra' then 'Non Astra' 
    ELSE 'check'
 END as sales_source_details,


current_timestamp() as insertion_timestamp, 


from {{ref('int_invoice_items')}} as ii 
)

select * from source

--where invoice_type != 'credit note' and generation_type !='MANUAL'

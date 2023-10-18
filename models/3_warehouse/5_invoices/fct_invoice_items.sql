with

source as ( 

select

--invoice Items

creditable_id,
invoice_item_generation_type,
    
    --fct
        price_without_tax,
        price,
        total_tax,
        quantity,
        total_cost,

        unit_price,
        unit_landed_cost,

case when invoice_header_printed_at is not null then 'Printed' else null end as printed_status,

---Gross Revenue: This is the total amount of revenue generated from all printed invoices in a given period, without considering any adjustments like credit notes.
    case when invoice_header_type = 'invoice' and invoice_item_status = 'APPROVED' then ii.price_without_tax else 0 end as gross_revenue,
    case when invoice_header_type = 'credit note' and invoice_item_status = 'APPROVED' then ii.price_without_tax else 0 end as credit_note,


case when date(invoice_header_printed_at) is not null then date(invoice_header_printed_at) else date(invoice_header_created_at) end as master_date,


--This represents the total monetary value deducted from the Gross Revenue for a specific period, such as a month, due to the issuance of credit notes. Credit notes are typically issued when a customer returns a product, doesn't accept a delivery, or when a correction to an invoice is required.
case when creditable_id is not null then 'creditable_id' else null end as creditable_id_check,

case when invoice_header_id is not null then 'invoice_header_id' else null end as invoice_header_id_check,


invoice_item_type_row,
creditable_type,



    --dim
        financial_administration, -- Market
        Customer,
        user_category, -- Segment
        debtor_number,




        invoice_item_id,
        drop_id, --concat(customer.debtor_number,ii.delivery_date)
        
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
        order_type,

        meta_supplier,

        product_category,
        product_subcategory,



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


 registered_clients,

trading_model,

feed_source_name,
line_item_id_check,
parent_id_check,

invoice_items_link,
invoice_link,
line_items_link,



current_timestamp() as insertion_timestamp, 


from {{ref('int_invoice_items')}} as ii 
)

select * from source

--where invoice_type != 'credit note' and generation_type !='MANUAL'

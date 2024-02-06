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

        gross_revenue,
        credit_note,

tamimi_rema_customer,

 CASE
            WHEN sales_source = 'Astra' and tamimi_rema_customer = 'Tamimi Customer' THEN 'Astra - Tamimi Sales'
            WHEN sales_source = 'Non Astra' and tamimi_rema_customer = 'Tamimi Customer' THEN 'Non Astra - Tamimi Sales'
            WHEN sales_source = 'Astra' and tamimi_rema_customer = 'REMA Customer' THEN 'Astra - REMA Sales'
            WHEN sales_source = 'Non Astra' and tamimi_rema_customer = 'REMA Customer' THEN 'Non Astra - REMA Sales'
            WHEN sales_source = 'Astra' then 'Astra'
            WHEN sales_source = 'Non Astra' then 'Non Astra' 
            ELSE 'To Be Scoped'
        END as sales_source_details,


case when sales_source = 'Non Astra' then gross_revenue else 0 end as non_astra_gross_revenue,
case when sales_source = 'Non Astra' then credit_note else 0 end as non_astra_credit_note,

case when sales_source = 'Astra' then gross_revenue else 0 end as astra_gross_revenue,
case when sales_source = 'Astra' then credit_note else 0 end as astra_credit_note,

case when sales_source = 'To Be Scoped' then gross_revenue else 0 end as tbs_gross_revenue,
case when sales_source = 'To Be Scoped' then credit_note else 0 end as tbs_credit_note,

-----
case when sales_source = 'Astra' and tamimi_rema_customer = 'Tamimi Customer' then gross_revenue else 0 end as astra_tamimi_gross_revenue,
case when sales_source = 'Astra' and tamimi_rema_customer = 'Tamimi Customer' then credit_note else 0 end as astra_tamimi_credit_note,

case when sales_source = 'Astra' and tamimi_rema_customer = 'REMA Customer' then gross_revenue else 0 end as astra_rema_gross_revenue,
case when sales_source = 'Astra' and tamimi_rema_customer = 'REMA Customer' then credit_note else 0 end as astra_rema_credit_note,

-----
case when sales_source = 'Non Astra' and tamimi_rema_customer = 'Tamimi Customer' then gross_revenue else 0 end as non_astra_tamimi_gross_revenue,
case when sales_source = 'Non Astra' and tamimi_rema_customer = 'Tamimi Customer' then credit_note else 0 end as non_astra_tamimi_credit_note,

case when sales_source = 'Non Astra' and tamimi_rema_customer = 'REMA Customer' then gross_revenue else 0 end as non_astra_rema_gross_revenue,
case when sales_source = 'Non Astra' and tamimi_rema_customer = 'REMA Customer' then credit_note else 0 end as non_astra_rema_credit_note,

----
case when sales_source = 'Non Astra' and tamimi_rema_customer = 'Normal Customer' then gross_revenue else 0 end as non_astra_normal_gross_revenue,
case when sales_source = 'Non Astra' and tamimi_rema_customer = 'Normal Customer' then credit_note else 0 end as non_astra_normal_credit_note,

case when sales_source = 'Astra' and tamimi_rema_customer = 'Normal Customer' then gross_revenue else 0 end as astra_normal_gross_revenue,
case when sales_source = 'Astra' and tamimi_rema_customer = 'Normal Customer' then credit_note else 0 end as astra_normal_credit_note,

-----


case when invoice_header_printed_at is not null then 'Printed' else null end as printed_status,

---Gross Revenue: This is the total amount of revenue generated from all printed invoices in a given period, without considering any adjustments like credit notes.
    case when invoice_header_type = 'credit note' and invoice_item_status = 'APPROVED'  then 1 else 0 end as credit_note_items_count,
    case when invoice_header_type = 'invoice' and invoice_item_status = 'APPROVED'  then 1 else 0 end as invoice_items_count,


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
        account_manager,
        warehouse,




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
        stock_model,

        meta_supplier,
        meta_supplier_code,
        meta_supplier_name,

        ordering_stock_type,

        product_category,
        product_subcategory,



--invoice Header

    --dim
        invoice_header_id,
        invoice_header_status, --draft, open, printed, signed, closed, canceled, rejected, voided
        invoice_header_type, --credit note, invoice
        generation_type,
        record_type,
        li_record_type_details,
        li_record_type,
        invoice_number,


    --date
        invoice_header_created_at,
        invoice_header_printed_at,

sales_source,


order_source,



 registered_clients,


feed_source_name,
line_item_id_check,
parent_id_check,

invoice_items_link,
invoice_link,
line_items_link,



pod_source_type,
trading_model,

stem_length,

current_timestamp() as insertion_timestamp, 


from {{ref('int_invoice_items')}} as ii 
)

select * from source

--where invoice_type != 'credit note' and generation_type !='MANUAL'

with

source as ( 

 
select


line_item_id,

--invoice Header
    invoice_header_created_at,
    invoice_header_printed_at, 
    invoice_header_status, --draft, open, printed, signed, closed, canceled, rejected, voided
    invoice_header_type,


invoice_item_status,

record_type,
invoice_item_type,
generation_type,
Customer,
Supplier,

product_name as Product,

fulfillment_mode,
order_status,
record_type_details,



financial_administration,
customer_type,




invoice_item_id,


source_type,


--date
    order_date,
    delivery_date,
    deleted_at,

--fct
    
    ordered_quantity,
    invoiced_quantity,
    fulfilled_quantity,
    
    price_without_tax,
    price,



   
proof_of_delivery_id_inv,
proof_of_delivery_id_line,
current_timestamp() as insertion_timestamp, 


from {{ref('int_invoice_items')}} as ii 
)

select * from source

--where invoice_type != 'credit note' and generation_type !='MANUAL'

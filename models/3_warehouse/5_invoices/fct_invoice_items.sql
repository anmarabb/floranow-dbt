with

source as ( 

 
select
case 
when invoice_type != 'credit note' and line_item_id is null then 'invoice_without_order'
else null end as s1,

line_item_id,
proforma_at,
printed_at, 
invoice_status, --draft, open, printed, signed, closed, canceled, rejected, voided

record_type,
invoice_type,
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

--fct
    
    ordered_quantity,
    invoiced_quantity,
    fulfilled_quantity,
    
    price_without_tax,



   
proof_of_delivery_id_inv,
proof_of_delivery_id_line,
current_timestamp() as insertion_timestamp, 


from {{ref('int_invoice_items')}} as ii 
)

select * from source

--where invoice_type != 'credit note' and generation_type !='MANUAL'

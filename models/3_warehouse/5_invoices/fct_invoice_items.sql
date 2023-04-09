with

source as ( 

 
select 
invoice_item_id,
line_item_id,
generation_type,
invoice_type,
source_type,

financial_administration,
order_date,
delivery_date,
Customer,
Supplier,

current_timestamp() as insertion_timestamp, 


from {{ref('int_invoice_items')}} as ii 
)

select * from source


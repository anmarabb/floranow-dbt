with

source as ( 


 
select 
additional_items_report_id,
status,
creation_stage,

--date
    delivery_date,
    created_at,
    failure_at,
    reported_at,
    approved_at,
    rejected_at,


currency,
fob_price,
quantity,
reported_by,
rejected_by,
approved_by,

warehouse,
feed_source,
Shipment,
order_type,
Customer,
Reseller,
order_source,
Origin,
debtor_number,
Supplier,
departure_date,
master_shipment,

line_item_id_check,
-- product_id_check,

product_link,
line_item_link,
additional_item_link,

product_name,
stem_length,

alternatable_id,
alternatable_type,

from {{ref('int_additional_items_reports')}} as ad 

)

select * from source

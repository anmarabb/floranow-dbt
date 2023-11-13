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
shipment,
order_type,

line_item_id_check,
product_id_check,

product_link,
line_item_link,
additional_item_link,

from {{ref('int_additional_items_reports')}} as ad 

)

select * from source

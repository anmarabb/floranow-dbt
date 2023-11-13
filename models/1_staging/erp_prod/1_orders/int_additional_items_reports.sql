with

source as ( 

SELECT 


ad.*,

reported_by.name as reported_by,
rejected_by.name as rejected_by,
approved_by.name as approved_by,


w.warehouse_name as warehouse,
fs.feed_source_name as feed_source,
sh.Shipment as shipment,

li.order_type,

case when li.line_item_id is not null then 'Line Item ID' else null end as line_item_id_check,
case when p.line_item_id is not null then 'Product ID' else null end as product_id_check,


concat( "https://erp.floranow.com/products/", ad.product_id) as product_link,
concat( "https://erp.floranow.com/line_items/", ad.line_item_id) as line_item_link,
concat( "https://erp.floranow.com/additional_items_reports/", ad.additional_items_report_id) as additional_item_link,


from  {{ref('stg_additional_items_reports')}} as ad
left join {{ ref('stg_products') }} as p on p.product_id = ad.product_id 
left join {{ ref('int_line_items')}} as li on ad.line_item_id = li.line_item_id
left join {{ref('base_users')}} as reported_by on reported_by.id = ad.reported_by_id
left join {{ref('base_users')}} as rejected_by on rejected_by.id = ad.rejected_by_id
left join {{ref('base_users')}} as approved_by on approved_by.id = ad.approved_by_id

left join {{ref('stg_shipments')}} as sh on ad.shipment_id = sh.shipment_id
left join {{ref('stg_feed_sources')}} as fs on fs.feed_source_id = ad.feed_source_id
left join {{ref('base_warehouses')}} as w on w.warehouse_id = sh.warehouse_id



)

select * from source


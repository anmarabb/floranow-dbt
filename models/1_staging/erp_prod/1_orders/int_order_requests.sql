select orr.*,
       s.supplier_name as Supplier,
       s.supplier_region, 
       w.warehouse_name as warehouse, 

from {{ref("stg_order_requests")}} as orr
left join {{ref("stg_feed_sources")}} fs on fs.feed_source_id = orr.feed_source_id
left join {{ref("base_suppliers")}} s on fs.supplier_id = s.supplier_id
left join {{ref("base_users")}} u on u.id = orr.customer_id
left join {{ref("base_warehouses")}} w on w.warehouse_id = u.warehouse_id
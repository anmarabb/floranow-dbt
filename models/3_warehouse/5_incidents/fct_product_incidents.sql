--granularty: multible incedint event per line item

with

source as ( 
        
select     
pi.line_item_id,

pi.incident_type,
pi.stage,
pi.quantity,
order_type,
    current_timestamp() as insertion_timestamp, 

from {{ ref('stg_product_incidents')}} as pi
left join {{ref('stg_line_items')}} as li on pi.line_item_id = li.id
left join {{ref('stg_products')}} as p on p.line_item_id = li.id 
left join {{ref('base_users')}} as u on u.id = li.customer_id
left join {{ref('base_suppliers')}} as stg_suppliers on stg_suppliers.id = li.supplier_id
left join {{ref('stg_order_requests')}} as orr on li.order_request_id = orr.id
left join {{ref('stg_shipments')}} as sh on li.shipment_id = sh.id
left join {{ref('stg_master_shipments')}} as msh on sh.master_shipment_id = msh.id
left join {{ref('stg_feed_sources')}} as fs on li.feed_source_id = fs.id
left join {{ref('stg_stocks')}} as stock on p.stock_id = stock.id 
left join {{ref('stg_warehouses')}} as w on w.id = stock.warehouse_id
left join {{ref('base_users')}} as reseller on reseller.id = p.reseller_id

where  pi.deleted_at is null
    )

select * from source
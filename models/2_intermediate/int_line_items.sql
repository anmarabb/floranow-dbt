with 
prep_registered_clients as (select financial_administration,count(*) as registered_clients from {{ ref('base_users') }} where account_type in ('External') group by financial_administration),   
prep_product_locations as (select  pl.locationable_id, max(pl.id) as id from {{ ref('stg_product_locations') }} as pl group by 1),
prep_picking_products as (select  pk.line_item_id, max(pk.id) as id from {{ ref('stg_picking_products') }} as pk group by 1)

SELECT


case when li.order_type = 'OFFLINE' and orr.standing_order_id is not null then 'STANDING' else li.order_type end as order_type,


prep_ploc.id as product_locations_id,
prep_picking_products.id as picking_products_id,
customer.name as customer,
user.name as user,

li.order_type as row_order_type,

li.* EXCEPT(order_type),


from {{ref('stg_line_items')}} as li
left join {{ ref('stg_products') }} as p on p.line_item_id = li.id 

left join {{ref('base_users')}} as customer on customer.id = li.customer_id
left join {{ref('base_users')}} as user on user.id = li.user_id

left join {{ ref('stg_proof_of_deliveries') }} as pod on li.proof_of_delivery_id = pod.id

left join {{ref('stg_shipments')}} as sh on li.shipment_id = sh.id
left join  {{ref('stg_master_shipments')}} as msh on sh.master_shipment_id = msh.id
left join {{ref('stg_invoices')}} as i on li.invoice_id = i.id
left join {{ref('stg_order_requests')}} as orr on li.order_request_id = orr.id
left join {{ref('stg_stocks')}} as stock on p.stock_id = stock.id 
left join {{ref('stg_warehouses')}} as w on w.id = customer.warehouse_id




left join prep_product_locations as prep_ploc on prep_ploc.locationable_id = p.id 
left join prep_picking_products as prep_picking_products on prep_picking_products.line_item_id = li.id
left join prep_registered_clients as prep_registered_clients on prep_registered_clients.financial_administration = customer.financial_administration

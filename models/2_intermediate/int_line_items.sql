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

dispatched_by.name as dispatched_by,
returned_by.name as returned_by,
created_by.name as created_by,
split_by.name as split_by,
order_requested_by.name as order_requested_by,


li.order_type as row_order_type,




pi.incidents_count,

case when li.line_item_type in ('Reselling Purchase Orders', 'EXTRA') and li.location = 'loc' and pi.incidents_count is  null then 1 else 0 end as Received_not_scanned,

li.* EXCEPT(order_type),


{% set  x = ['missing_quantity', 'delivered_quantity','inventory_quantity','warehoused_quantity','picked_quantity','fulfilled_quantity','received_quantity','quantity','returned_quantity','splitted_quantity','replaced_quantity','extra_quantity','damaged_quantity','published_canceled_quantity'] %}
{% for x in x %}
case 
    when li.{{x}} > 0 then '{{x}}'
    when li.{{x}} = 0 then '--'
end as ch_{{x}}
        {%- if not loop. last -%}
        ,
        {%- endif -%}
        {% endfor -%},


{% set  x = ['updated_at', 'created_at','completed_at','departure_date','delivery_date','deleted_at','split_at','canceled_at','delivered_at','dispatched_at','returned_at','order_id','offer_id','root_shipment_id','shipment_id','source_shipment_id','split_source_id','replace_for_id','feed_source_id','customer_master_id','customer_id','user_id','reseller_id','supplier_id','created_by_id','split_by_id','returned_by_id','canceled_by_id','dispatched_by_id','supplier_product_id','order_request_id','order_payload_id','source_invoice_id','invoice_id','proof_of_delivery_id','parent_line_item_id','source_line_item_id','line_item_id','sequence_number','number','variety_mask','product_mask','barcode','previous_moved_proof_of_deliveries','previous_split_proof_of_deliveries','previous_shipments'] %}
{% for x in x %}
case 
    when li.{{x}} is not null then '{{x}}'
    when li.{{x}} is null then '--'
end as ch_{{x}}
        {%- if not loop. last -%}
        ,
        {%- endif -%}
        {% endfor -%},  




s.supplier_name,

s.supplier_region,


from {{ref('stg_line_items')}} as li
left join {{ ref('stg_products') }} as p on p.line_item_id = li.line_item_id 
left join {{ref('stg_order_requests')}} as orr on li.order_request_id = orr.id


left join {{ref('base_users')}} as customer on customer.id = li.customer_id
left join {{ref('base_users')}} as user on user.id = li.user_id
left join {{ref('base_users')}} as dispatched_by on dispatched_by.id = li.dispatched_by_id
left join {{ref('base_users')}} as returned_by on returned_by.id = li.returned_by_id
left join {{ref('base_users')}} as created_by on created_by.id = li.created_by_id
left join {{ref('base_users')}} as split_by on split_by.id = li.split_by_id
left join {{ref('base_users')}} as order_requested_by on order_requested_by.id = orr.created_by_id

left join {{ref('base_suppliers')}} as s on s.supplier_id = li.supplier_id


left join {{ ref('stg_proof_of_deliveries') }} as pod on li.proof_of_delivery_id = pod.id

left join {{ref('stg_shipments')}} as sh on li.shipment_id = sh.id
left join  {{ref('stg_master_shipments')}} as msh on sh.master_shipment_id = msh.id
left join {{ref('stg_invoices')}} as i on li.invoice_id = i.id
left join {{ref('base_stocks')}} as stock on p.stock_id = stock.stock_id 

left join {{ref('base_warehouses')}} as w on w.warehouse_id = customer.warehouse_id


left join {{ ref('fct_product_incidents_groupby_order_line') }} as pi on pi.line_item_id = li.line_item_id



left join prep_product_locations as prep_ploc on prep_ploc.locationable_id = p.id 
left join prep_picking_products as prep_picking_products on prep_picking_products.line_item_id = li.line_item_id
left join prep_registered_clients as prep_registered_clients on prep_registered_clients.financial_administration = customer.financial_administration

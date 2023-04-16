with 
prep_registered_clients as (select financial_administration,count(*) as registered_clients from {{ ref('base_users') }} where account_type in ('External') group by financial_administration),   
prep_product_locations as (select  pl.locationable_id, max(pl.product_location_id) as product_location_id from {{ ref('stg_product_locations') }} as pl group by 1),
prep_picking_products as (select  pk.line_item_id, max(pk.picking_product_id) as picking_product_id from {{ ref('stg_picking_products') }} as pk group by 1)

SELECT
li.* EXCEPT(order_type,delivery_date),

case when li.order_type = 'OFFLINE' and orr.standing_order_id is not null then 'STANDING' else li.order_type end as order_type,
case when li.delivery_date is null and li.order_type in ('IMPORT_INVENTORY', 'EXTRA','MOVEMENT') then date(li.created_at) else li.delivery_date end as delivery_date,


case when li.record_type_details in ('Reseller Purchase Order', 'EXTRA') and li.location = 'loc' and pi.incidents_count is  null then 1 else 0 end as Received_not_scanned,


--funnel touchpoints 
    case when li.received_quantity > 0 then 1 else 0 end as order_received,
    case when li.fulfilled_quantity > 0 then 1 else 0 end as order_fulfilled,
    case when li.location = 'pod' then 1 else 0 end as order_pod_moved,
    case when li.dispatched_at is not null then 1 else 0 end as order_dispatched,
    case when li.state = 'DELIVERED' then 1 else 0 end as order_delivered,
    case when li.invoice_id is not null then 1 else 0 end as invoice_created,
    case when li.invoice_id is not null and i.printed_at is not null then 1 else 0 end as invoice_printed,


    case when li.location = 'loc' then 1 else 0 end as order_loc_moved, --order_warehoused
    case when li.picked_quantity > 0 then 1 else 0 end as order_picked,





--date
    date.dim_date,


--customer
    user.name as user,
    customer.name as customer,
    customer.country,
    customer.financial_administration,
    customer.account_manager,
    customer.debtor_number,
    customer.customer_type,

    case when customer.debtor_number in ('WANDE','95110') then 'Internal Invoicing' else 'Normal Invoicing' end as internal_invoicing,

    case when li.received_quantity > 0 then 'Received' else 'Not Received' end as ops_status1,
    case when li.state in ('PENDING','CANCELED') then 'Not Fulfilled' else 'Fulfilled' end as ops_status2,
    case when li.location = 'pod' then 'Prepared' else 'Not Prepared' end as ops_status3,
    case when li.dispatched_at is not null then 'Dispatched' else 'Not Dispatched' end as ops_status4,
    case when li.state = 'DELIVERED' then 'Signed' else 'Not Signed' end as ops_status5,


    concat( "https://erp.floranow.com/line_items/", li.line_item_id) as line_item_link,

    returned_by.name as returned_by,


--supplier
    case when li.parent_line_item_id is not null then plis.supplier_name else lis.supplier_name end as Supplier,
    lis.supplier_region,

--order 
    pli.order_type as parent_order_type,

    case 
        when li.record_type_details in ('Customer Fly Order','Customer Shipment Order') then 'Shipment Orders'  -- From Shipment External
        when li.record_type_details in ('Customer Inventory Order') then 'Inventory Orders (Stock-out)'                     -- From Inventory (stock out)
        when li.record_type_details in ('Reseller Purchase Order','EXTRA','RETURN') then 'Reselling Orders (Stock-in)'      -- PO Orders (in) To Inventory Replenishment, Restocking
        else null
        end as fulfillment_mode,


    case 
     when li.state = 'CANCELED' then '1. Not Fulfilled - (Canceled Orders)'
     when li.location is null and li.order_type = 'IN_SHOP' and li.fulfillment = 'SUCCEED' then '5. Fulfilled - In Shop'
     when li.location = 'loc' and li.fulfillment = 'SUCCEED' then '4. Fulfilled - Warehoused Totaly'                                          --  Moveded Totaly to Stock (Warehoused)
     when li.location = 'loc' and li.fulfillment = 'PARTIAL' then '4. Fulfilled - Warehoused Partially (with Incidents)'                      --  Moveded Partially to Stock (Warehoused)
     when li.location = 'loc' and li.fulfillment = 'UNACCOUNTED' then '4. Fulfilled - Warehoused (with Process Breakdown)'
     when li.location = 'pod' and li.fulfillment = 'SUCCEED' then '3. Fulfilled - Moved Totaly to POD'                                        --  Moveded Totaly to Dispatch Area (pod)
     when li.location = 'pod' and li.fulfillment = 'PARTIAL' then '3. Fulfilled - Moved Partially to POD (with Incidents)'                    --  Moveded Partially to Dispatch Area (pod)
     when li.location = 'pod' and li.fulfillment = 'UNACCOUNTED' then '3. Fulfilled - Moved to POD (with Process Breakdown)'
     when li.location is null and li.state != 'CANCELED' and li.fulfillment = 'FAILED' then '2. Fulfilled - with Full Item Incident'
     when li.location is null and li.state != 'CANCELED' and li.fulfillment = 'UNACCOUNTED' then '1. Not Fulfilled - (Investigate)'
     when li.location is null and li.fulfillment in ('PARTIAL','SUCCEED') then '3. Fulfilled - with Process Breakdown'
     else 'cheack_my_logic'  
     end as fulfillment_status,
             

 
--order requist
    orr.status as order_request_status,

--order_payloads
    --opl.offer_id,
    opl.status as order_payloads_status,


--shipments
    sh.status as shipments_status, 
    msh.status as master_shipments_status,

w.warehouse_name as warehouse,


pi.incidents_count,



pod.source_type,
pod.pod_status,
pod.dispatched_by,


case 
    when date_diff(date(li.delivery_date)  ,current_date(), month) > 1 then 'Wrong date' 
    when li.delivery_date > current_date() then "Future" 
    when li.delivery_date = current_date() then "Today" 
    when li.delivery_date < current_date()-1 then "Past" 
    else "cheak" end as select_delivery_date,

/*

--p.product_id,
--pp.product_id as pp_product_id,
--li.parent_line_item_id,
--lis.supplier_name as lis_supplier_name,
--plis.supplier_name as plis_supplier_name,


prep_ploc.id as product_locations_id,
prep_picking_products.id as picking_products_id,


dispatched_by.name as dispatched_by,
created_by.name as created_by,
split_by.name as split_by,
order_requested_by.name as order_requested_by,


li.order_type as row_order_type,
*/








/*
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
*/


--Metreics
   -- count (distinct li.order_number ) as orders,
   -- count (distinct li.line_item_id ) as line_orders,





from {{ref('stg_line_items')}} as li
left join {{ ref('stg_products') }} as p on p.line_item_id = li.line_item_id 
left join {{ref('stg_order_requests')}} as orr on li.order_request_id = orr.id
left join {{ref('stg_order_payloads')}} as opl on li.order_payload_id = opl.order_payload_id


left join {{ref('stg_invoice_items')}} as ii on ii.line_item_id=li.line_item_id and ii.invoice_type = 'invoice'
left join {{ref('base_users')}} as customer on customer.id = li.customer_id
left join {{ref('base_users')}} as user on user.id = li.user_id
left join {{ref('base_users')}} as dispatched_by on dispatched_by.id = li.dispatched_by_id
left join {{ref('base_users')}} as returned_by on returned_by.id = li.returned_by_id
left join {{ref('base_users')}} as created_by on created_by.id = li.created_by_id
left join {{ref('base_users')}} as split_by on split_by.id = li.split_by_id
left join {{ref('base_users')}} as order_requested_by on order_requested_by.id = orr.created_by_id

left join {{ref('base_suppliers')}} as lis on lis.supplier_id = li.supplier_id

left join {{ ref('stg_products') }} as pp on pp.line_item_id = li.parent_line_item_id 
left join {{ref('stg_line_items')}} as pli on pli.line_item_id = li.parent_line_item_id
left join {{ref('base_suppliers')}} as plis on plis.supplier_id = pli.supplier_id


left join {{ ref('dim_proof_of_deliveries') }} as pod on li.proof_of_delivery_id = pod.proof_of_delivery_id

left join {{ref('stg_shipments')}} as sh on li.shipment_id = sh.id
left join  {{ref('stg_master_shipments')}} as msh on sh.master_shipment_id = msh.id
left join {{ref('stg_invoices')}} as i on li.invoice_id = i.invoice_id

left join {{ref('base_stocks')}} as st on p.stock_id = st.stock_id 
--left join {{ref('stg_feed_sources')}} as origin_fs on origin_fs.feed_source_id = p.origin_feed_source_id


left join {{ref('base_warehouses')}} as w on w.warehouse_id = customer.warehouse_id


left join {{ref('fct_product_incidents_groupby_order_line')}} as pi on pi.line_item_id = li.line_item_id


left join {{ref('stg_additional_items_reports')}}  as ad on ad.line_item_id=li.line_item_id

left join {{ref('dim_date')}}  as date on date.dim_date = date(li.created_at)


left join prep_product_locations as prep_ploc on prep_ploc.locationable_id = p.product_id 
left join prep_picking_products as prep_picking_products on prep_picking_products.line_item_id = li.line_item_id
left join prep_registered_clients as prep_registered_clients on prep_registered_clients.financial_administration = customer.financial_administration

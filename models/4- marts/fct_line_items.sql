
with 
prep_product_incidents as (select line_item_id, count(*) as incidents_count, sum(case when  pi.stage in('PACKING','RECEIVING') then pi.quantity else 0 end) as extra_quantity, from `floranow.erp_prod.product_incidents` as pi group by line_item_id  having extra_quantity<0 ),
prep_registered_clients as (select financial_administration,count(*) as registered_clients from `floranow.Floranow_ERP.users` where account_type in ('External') group by financial_administration)   
SELECT

case when li.parent_line_item_id is not null then parent_li.unit_fob_price else li.unit_fob_price end as unit_fob_price_2,
case when li.parent_line_item_id is not null then parent_li.fob_currency else li.fob_currency end as fob_currency_2,

li.unit_fob_price,
li.fob_currency,
parent_li.unit_fob_price as root_unit_fob_price,
parent_li.fob_currency as root_fob_currency,




case 
    when stg_users.customer_type = 'reseller' then 'Resale Trading'
    when stg_users.customer_type = 'cif' then 'Bulk Trading'
    when stg_users.customer_type = 'fob' then 'Bulk Trading'
    when stg_users.customer_type = 'retail' then 'Traditional Trading'
    else 'check_my_logic'
end as trading_model,





li.fulfillment,

li.stem_length,
li.color,
li.pn.p1 as spec_1,
li.pn.p2 as spec_2,
li.pn.p3 as spec_3,
li.pn.p4 as spec_4,
li.Properties,
li.categorization,
li.sales_unit,

product_incidents.line_item_id as incident, 
--ARRAY_AGG(Struct(specification.specification_name as spec_name, sp_Values.flori_value_name as spec_Value)) as Spec






case 
when li_suppliers.supplier_name in ('wish flower','ASTRA Farms','Fulfilled by Floranow','Fulfilled by Floranow SA','The Orchid Garden','Solai Roses','Selemo Valley Farms','Lomalinda','Gallica','Galleria Farms','Fresh Cap','Florius','Flores Del Este','Floranow Holland','Elite Flower Farm','Ecoflor','Capiro','Agroindustria','Smithers Oasis')
and date_diff( cast(li.delivery_date  as date ),cast(li.created_at as date), DAY) in (0,1) then 'Express'
else 'Regular'
end as delivery_method,

li.source_line_item_id, 
li.parent_line_item_id,
    stg_users.customer,

li.pricing_type,


    stg_users.client_category,
    stg_users.customer_type,
    stg_users.payment_term,
    stg_users.financial_administration,



li.id,

concat( "https://erp.floranow.com/line_items/", li.id) as line_item_link,

li.state,

li.tags,

li.unit_landed_cost,
li.landed_currency,


li.unit_price,
li.currency,

li.total_price_without_tax,


li.product_name as product,

li.order_number,
li.order_type as row_order_type,

--date
    li.departure_date,
    li.delivery_date,
    li.created_at,

li.order_id,

/*
replace_for_id
proof_of_delivery_id

li.order_request_id,
li.source_line_item_id, 
li.parent_line_item_id,

li.shipment_id,
li.source_shipment_id,
li.root_shipment_id,
*/

--julia




--Feed_Source table


---shipments table
    sh.name as shipment,
    msh.name as master_shipment_name,
    



--proof of delivery table
    case when li.proof_of_delivery_id is not null then 'POD' else 'null' end as proof_of_delivery,
    pod.status as pod_status,

    li.proof_of_delivery_id,

    pod.source_type as pod_source_type,


--quantity
    li.quantity,  --conformed_quantity from supplier
    li.fulfilled_quantity, --received and valied excluding extra --- minace extra
    li.received_quantity, --received and valied including extra, 
li.quantity - li.fulfilled_quantity as gap_quantity,

    li.quantity as li_quantity,
    p.quantity as p_quantity,




li.creation_stage,

    li.inventory_quantity,
    li.missing_quantity,
    li.damaged_quantity,
    li.delivered_quantity,
    li.extra_quantity,

    abs(product_incidents.extra_quantity) as calc_extra_quantity, --Abi
    
    
    --product_incidents.extra_quantity + li.fulfilled_quantity as  valid_received_quantity, calculate it in datastudio.




    li.returned_quantity,
    li.canceled_quantity,
    li.picked_quantity,


--line items custom metrics
    case when li.supplier_id IN (109,71) then 'Express' when li.supplier_id is null then "Check My Logic" else 'NonExpress' end as order_mode,
  
    case 
    when date_diff(date(li.delivery_date)  ,current_date(), month) > 1 then 'Wrong date' 
    when li.delivery_date > current_date() then "Future" 
    when li.delivery_date = current_date() then "Today" 
    else "Past" end as future_delivery_date,

    case 
    when date_diff(date(li.departure_date)  ,current_date(), month) > 1 then 'Wrong date' 
    when li.departure_date = current_date() then "Today" 
    when li.departure_date > current_date() then "Future" 
    when li.departure_date < current_date() then "Past" 

    else "check my logic" end as departure_timing,

    

    case when EXTRACT(HOUR FROM li.created_at) in (1,2,3,4,5,6) then "5_to_10_time_slote" else "otheres" end as time_slot,
    case when li.supplier_id IN (109,71) then li.total_price_without_tax else 0 end as express_sales,

    case when li.split_at is not null then 'split' else 'not-split' end as split_status,
    case when li.returned_at is not null then 'returned' else ' ' end as returned_status,



    case 
        when date_diff( cast(li.delivery_date  as date ),cast(li.created_at as date), DAY) = 0 then 'Same day express'
        when date_diff( cast(li.delivery_date  as date ),cast(li.created_at as date), DAY) = 1 then 'Next day express delivery'
        when date_diff( cast(li.delivery_date  as date ),cast(li.created_at as date), DAY) > 1 then 'Regular delivery'
        else 'check my logic'
    end as delivery_type,




    li.quantity*li.unit_fob_price as potential_fob_revenue,
    li.fulfilled_quantity*li.unit_fob_price as fulfilled_fob_revenue,
    (li.quantity*li.unit_fob_price) - (li.fulfilled_quantity*li.unit_fob_price) as fob_missed_revenue,

    case when li.id is not null then li.total_price_without_tax else 0 end as potential_revenue,
    case when li.id is not null then li.fulfilled_quantity * li.unit_price else 0 end as fulfilled_revenue,
    case when li.invoice_id is not null and i.printed_at is not null then li.fulfilled_quantity * li.unit_price else 0 end as invoiced_revenue,
    case when li.invoice_id is not null and i.printed_at is null then li.fulfilled_quantity * li.unit_price else 0 end as Proforma_revenue,

--cost calculation
    li.quantity * li.unit_landed_cost as potential_cost,
    li.fulfilled_quantity * li.unit_landed_cost as fulfilled_cost,
    li.total_price_without_tax - li.quantity * li.unit_landed_cost as potential_profit,
    li.fulfilled_quantity * li.unit_price - li.fulfilled_quantity * li.unit_landed_cost as actual_profit,



--  order requests table
case when li.order_type = 'OFFLINE' and orr.standing_order_id is not null then 'STANDING' else li.order_type end as order_type,


--stg_users
    stg_users.city,
    stg_users.account_manager,
    stg_users.country,
    stg_users.reseller,
    stg_users.retail,
    stg_users.debtor_number,
 

--invoice table.
    i.printed_at,
    case when date_diff(cast (li.delivery_date as date) ,cast(i.printed_at as date), MONTH) = 0 then 'ok' else 'moved_to_next_month_invoice' end as financal_month,
    case when date(i.printed_at) > li.delivery_date then "late_delivery" else "ontime_delivery" end as late_or_ontime_delivery,
    CASE WHEN date(i.printed_at) > li.delivery_date then 'late_delivery' else 'on_time_delivery' End as otd_check,

    case 
        when li.invoice_id is not null and i.printed_at is not null then 'Printed Invoice' 
        when li.invoice_id is not null and i.printed_at is null then 'Proforma Invoice'
        else 'Not Printed Invoice' 
    end as invoice_status,

    case 
        when i.payment_status = 0 then "Not Paid" 
        when i.payment_status = 1 then "partially_paid " 
        when i.payment_status = 2 then "Not Paid" 
        else 'Not invoiced' 
    End as payment_status,



prep_registered_clients.registered_clients,

/*
client_orders_from_express
client_orders_from_marketplace
resllers_orders_from_marketplace
resllers_orders_from_express

case 
    when u.customer_type = 0 then 'reseller'
    when u.customer_type = 1 then 'retail'
    when u.customer_type = 2 then 'fob'
    when u.customer_type = 3 then 'cif'
    else 'check_my_logic'
    end as customer_type,

*/

--ordering source type (external, flying ,envetry)

concat(stg_users.debtor_number,li.delivery_date) as drop_id, 


case 
when li_suppliers.supplier_name = 'ASTRA Farms' then 'Astra'
when li_suppliers.supplier_name = 'Fulfilled by Floranow SA' and li_fs.name in ('Express Jeddah','Express Dammam','Express Riyadh','Express Tabuk')  then 'Astra'
else 'Non Astra'
end as sales_source,


li.category as item_category,
li.category2 as item_sub_category,

stock.name as stock_name,

case when w.name is not null then w.name  end as warehouse,
 w.country as warehouse_country,




case
    when li.order_type in ('ADDITIONAL') and msh.name is null  then 'ADDITIONAL Not from shipment'
    when li.order_type in ('EXTRA') and msh.name is null  then 'EXTRA Not from shipment'
    when li.order_type in ('IMPORT_INVENTORY') then 'IMPORT_INVENTORY'
    when li.order_type in ('RETURN') then 'RETURN'
    when li.ordering_stock_type is null and li.state not in ('CANCELED') and li.order_type not in ('IMPORT_INVENTORY','RETURN')  then 'Vendor Performance Report'
    else 'check_my_logic'
 end as report_filter,


case 
when li.source_line_item_id is null and li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is not null then '1- reselling_purchase_orders'
when li.source_line_item_id is null and li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is null then '3- customer_direct_orders'
when li.source_line_item_id is null and li.ordering_stock_type is not null and li.reseller_id is null then '4- customer_inventory_orders'

when li.source_line_item_id is null and li.ordering_stock_type is not null and li.reseller_id is not null then 'stock2stock'
when li.source_line_item_id is not null and li.order_type = 'EXTRA' then 'EXTRA'
when li.source_line_item_id is not null and li.order_type = 'RETURN' then 'RETURN' 
when li.source_line_item_id is not null and li.order_type = 'MOVEMENT' then 'MOVEMENT'


else 'cheack_my_logic'
end as persona,


case
    when li.parent_line_item_id is not null then null
    when li.ordering_stock_type is not null then null
    when li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is not null and li.order_type in ('IMPORT_INVENTORY') then 're-selling path (IMPORT_INVENTORY)'
    when li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is null then 'pre-selling path'
    when li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is not null then 're-selling path'

    else 'check_my_logic'
 end as report_filter_vendor,


case
    when li.parent_line_item_id is null then null
    when li.ordering_stock_type is null then null
    when li.parent_line_item_id is not null and li.ordering_stock_type is not null and li.reseller_id is not null then 'reseller from stock'
    when li.parent_line_item_id is not null and li.ordering_stock_type is not null and li.reseller_id is null then 'client from stock'
    else 'check_my_logic'
 end as report_filter_stock,

li.ordering_stock_type,

case
when li.order_type in ('EXTRA') and li.creation_stage in ('INVENTORY') and msh.name is not null then 'EXTRA_SHIPMENT_INVENTORY'
when li.order_type in ('EXTRA') and li.creation_stage in ('PACKING') and msh.name is not null then 'EXTRA_SHIPMENT_PACKING'
when li.order_type in ('EXTRA') and li.creation_stage in ('INVENTORY') and msh.name is  null then 'EXTRA_IMPORT_INVENTORY'
when li.order_type in ('EXTRA') and li.creation_stage in ('PACKING') and msh.name is  null then 'EXTRA_IMPORT_PACKING'
 else 'NOT_EXTRA'
 end as Extra_type,


li.packaging,






case 
when li.ordering_stock_type = 'INVENTORY' and li.parent_line_item_id is not null then 'INVENTORY'
when li.ordering_stock_type = 'FLYING' and li.parent_line_item_id is not null then 'FLYING'
when li.ordering_stock_type in ('INVENTORY','FLYING') and li.parent_line_item_id is null then 'problem'
when li.ordering_stock_type is null and li.parent_line_item_id is null then null
end as calc_ordering_stock_type,





li_suppliers.account_manager as supplier_account_manager,
li_suppliers.supplier_region,
li_suppliers.supplier_type,



li_suppliers.supplier_name,
li_fs.name as feed_source,


p_suppliers.supplier_name as p_supplier,
p_fs.name as p_feed_source,
p_origin_fs.name as p_origin_feed_source,

--parent
parent_li_suppliers.supplier_name as parent_li_supplier,
parent_li_fs.name as parent_li_feed_source,

p_parent_li_suppliers.supplier_name as p_parent_li_supplier,
p_parent_li_fs.name as p_parent_li_feed_source,
p_parent_li_origin_fs.name as p_parent_li_origin_feed_source,

case 
when p_suppliers.supplier_name = li_suppliers.supplier_name then 'Same supplier' 
when p_suppliers.supplier_name != li_suppliers.supplier_name then 'Transformed supplier' 
end as supplier_transform,




parent_li.order_type as parent_li_order_type,


    case when li.parent_line_item_id is null then null else 'have_parent_line_item_id' end as parent_line_item_id_cheack,
    case when li.ordering_stock_type is null then null else 'Inventory_order' end as ordering_stock_type_cheack,
    case when li.invoice_id is null then null else 'have_invoice_id' end as invoice_id_cheack,
    case when p.line_item_id is null then null else  'have_product_id' end as product_id_cheack,

    case when li.source_line_item_id is null then null else 'have_source_line_item_id' end as source_line_item_id_cheack,

    case when li.shipment_id is null then null else 'have_shipment_id' end as shipment_id_cheack,
    case when li.root_shipment_id is null then null else 'have_root_shipment_id' end as root_shipment_id_cheack,
    case when li.source_shipment_id is null then null else 'have_source_shipment_id' end as source_shipment_id_cheack,







users.name as user,







FROM {{ source('erp_prod', 'line_items') }} As li
--fetsh data from (line_items)
    left join `floranow.Floranow_ERP.suppliers` as li_suppliers on li_suppliers.id = li.supplier_id
    left join `floranow.erp_prod.feed_sources` as li_fs on li.feed_source_id = li_fs.id

--fetsh data from (parent line_items)
    left join  `floranow.erp_prod.line_items` as parent_li on parent_li.id = li.parent_line_item_id
    left join `floranow.Floranow_ERP.suppliers` as parent_li_suppliers on parent_li_suppliers.id = parent_li.supplier_id
    left join `floranow.erp_prod.feed_sources` as parent_li_fs on parent_li.feed_source_id = parent_li_fs.id

--fetsh data from (products)
    left join floranow.erp_prod.products as p on p.line_item_id = li.id
    left join `floranow.Floranow_ERP.suppliers` as p_suppliers on p_suppliers.id = p.supplier_id
    left join `floranow.erp_prod.feed_sources` as p_fs on p_fs.id = p.feed_source_id
    left join `floranow.erp_prod.feed_sources` as p_origin_fs on p_origin_fs.id = p.origin_feed_source_id 


   -- left join floranow.erp_prod.products as p_parent_li on p_parent_li.line_item_id = li.parent_line_item_id
    left join floranow.erp_prod.products as p_parent_li on p_parent_li.line_item_id = parent_li.id
    left join `floranow.Floranow_ERP.suppliers` as p_parent_li_suppliers on p_parent_li_suppliers.id = p_parent_li.supplier_id
   left join `floranow.erp_prod.feed_sources` as p_parent_li_fs on p_parent_li_fs.id = p_parent_li.feed_source_id
    left join `floranow.erp_prod.feed_sources` as p_parent_li_origin_fs on p_parent_li_origin_fs.id = p_parent_li.origin_feed_source_id 







left join `floranow.Floranow_ERP.users` as stg_users on stg_users.id = li.customer_id

left join `floranow.erp_prod.proof_of_deliveries` as pod on li.proof_of_delivery_id = pod.id
left join `floranow.erp_prod.shipments` as sh on li.shipment_id = sh.id
left join  `floranow.erp_prod.master_shipments` as msh on sh.master_shipment_id = msh.id
left join `floranow.erp_prod.invoices` as i on li.invoice_id = i.id
left join prep_product_incidents AS product_incidents ON product_incidents.line_item_id = li.id
left join `floranow.erp_prod.order_requests` as orr on li.order_request_id = orr.id



left join prep_registered_clients as prep_registered_clients on prep_registered_clients.financial_administration = stg_users.financial_administration


left join `floranow.erp_prod.stocks` as stock on p.stock_id = stock.id 
--left join `floranow.erp_prod.warehouses` as w on msh.warehouse_id = w.id
left join `floranow.erp_prod.warehouses` as w on w.id = stock.warehouse_id



left join `floranow.erp_prod.packages` as packages on packages.shipment_id = sh.id and packages.sub_master_shipment_id = msh.id
left join `floranow.erp_prod.package_line_items` as packages_li on packages_li.line_item_id  = li.id  and packages_li.package_id = packages.id
left join `floranow.erp_prod.packing_box_items` as packbox on packbox.shipment_id = sh.id and packbox.line_item_number =li.number and packbox.package_number = packages.number
left join `floranow.erp_prod.packing_lists` as packlist on packlist.shipment_id = sh.id and packlist.supplier_id = li.supplier_id and packlist.departure_date = li.departure_date


left join floranow.erp_prod.users as users on users.id = li.user_id

--where li.ordering_stock_type is null
--stg_users.client_category = 'Internal-UAE'
--stg_users.customer_type = 'reseller'
--where stg_users.financial_administration = 'Internal'
--where stg_users.payment_term = 'Without invoicing'
WITH color_list AS (  
SELECT lower('Light Purple') as color 
 UNION ALL SELECT lower('Peach') as color 
 UNION ALL SELECT lower('Transparent') as color 
 UNION ALL SELECT lower('fuchsia') as color 
 UNION ALL SELECT lower('mixed in tray') as color 
 UNION ALL SELECT lower('sand') as color 
 UNION ALL SELECT lower('dark brown') as color 
 UNION ALL SELECT lower('red brown') as color 
 UNION ALL SELECT lower('green brown') as color 
 UNION ALL SELECT lower('white lilac') as color 
 UNION ALL SELECT lower('classic red') as color 
 UNION ALL SELECT lower('color supported') as color 
 UNION ALL SELECT lower('white') as color 
 UNION ALL SELECT lower('dark red') as color 
 UNION ALL SELECT lower('White Green') as color 
 UNION ALL SELECT lower('light blue') as color 
 UNION ALL SELECT lower('orange brown') as color 
 UNION ALL SELECT lower('light yellow') as color 
 UNION ALL SELECT lower('light green') as color 
 UNION ALL SELECT lower('beige') as color 
 UNION ALL SELECT lower('blued') as color 
 UNION ALL SELECT lower('blue white') as color 
 UNION ALL SELECT lower('white purple') as color 
 UNION ALL SELECT lower('yellow') as color 
 UNION ALL SELECT lower('cerise') as color 
 UNION ALL SELECT lower('blue') as color 
 UNION ALL SELECT lower('bicolour') as color 
 UNION ALL SELECT lower('violet') as color 
 UNION ALL SELECT lower('red yellow') as color 
 UNION ALL SELECT lower('lemon') as color 
 UNION ALL SELECT lower('Pink flamed') as color 
 UNION ALL SELECT lower('grey') as color 
 UNION ALL SELECT lower('Mix') as color 
 UNION ALL SELECT lower('champagne') as color 
 UNION ALL SELECT lower('pink green') as color 
 UNION ALL SELECT lower('teal') as color 
 UNION ALL SELECT lower('Coffee') as color 
 UNION ALL SELECT lower('Red/Pink') as color 
 UNION ALL SELECT lower('purple blue') as color 
 UNION ALL SELECT lower('red pink') as color 
 UNION ALL SELECT lower('apricot') as color 
 UNION ALL SELECT lower('pink purple') as color 
 UNION ALL SELECT lower('pale yellow') as color 
 UNION ALL SELECT lower('green grey') as color 
 UNION ALL SELECT lower('purple green') as color 
 UNION ALL SELECT lower('bicolour pink-cream') as color 
 UNION ALL SELECT lower('tricolor') as color 
 UNION ALL SELECT lower('silver') as color 
 UNION ALL SELECT lower('white red') as color 
 UNION ALL SELECT lower('Bicolor pink') as color 
 UNION ALL SELECT lower('Wooden color') as color 
 UNION ALL SELECT lower('bronze') as color 
 UNION ALL SELECT lower('classic dark purple') as color 
 UNION ALL SELECT lower('grey green') as color 
 UNION ALL SELECT lower('chocolate') as color 
 UNION ALL SELECT lower('classic purple') as color 
 UNION ALL SELECT lower('blue purple') as color 
 UNION ALL SELECT lower('tinted') as color 
 UNION ALL SELECT lower('dark yellow') as color 
 UNION ALL SELECT lower('aubergine') as color 
 UNION ALL SELECT lower('magenta') as color 
 UNION ALL SELECT lower('green lilac') as color 
 UNION ALL SELECT lower('coral') as color 
 UNION ALL SELECT lower('marble') as color 
 UNION ALL SELECT lower('copper') as color 
 UNION ALL SELECT lower('pink') as color 
 UNION ALL SELECT lower('dark pink') as color 
 UNION ALL SELECT lower('red green') as color 
 UNION ALL SELECT lower('purple red') as color 
 UNION ALL SELECT lower('four color') as color 
 UNION ALL SELECT lower('classic green') as color 
 UNION ALL SELECT lower('various colours') as color 
 UNION ALL SELECT lower('mixed') as color 
 UNION ALL SELECT lower('lavender') as color 
 UNION ALL SELECT lower('Light Pink') as color 
 UNION ALL SELECT lower('bicolour orange-yellow') as color 
 UNION ALL SELECT lower('Pink White') as color 
 UNION ALL SELECT lower('100% blue') as color 
 UNION ALL SELECT lower('oldrose') as color 
 UNION ALL SELECT lower('dark purple') as color 
 UNION ALL SELECT lower('gold') as color 
 UNION ALL SELECT lower('yellow red') as color 
 UNION ALL SELECT lower('light red') as color 
 UNION ALL SELECT lower('brown red') as color 
 UNION ALL SELECT lower('green red') as color 
 UNION ALL SELECT lower('Semaforo') as color 
 UNION ALL SELECT lower('dark pink red') as color 
 UNION ALL SELECT lower('orange red') as color 
 UNION ALL SELECT lower('yellow green') as color 
 UNION ALL SELECT lower('green pink') as color 
 UNION ALL SELECT lower('dark green') as color 
 UNION ALL SELECT lower('green purple') as color 
 UNION ALL SELECT lower('hot pink') as color 
 UNION ALL SELECT lower('black white') as color 
 UNION ALL SELECT lower('light gray') as color 
 UNION ALL SELECT lower('purple') as color 
 UNION ALL SELECT lower('mixed colours') as color 
 UNION ALL SELECT lower('white pink') as color 
 UNION ALL SELECT lower('terracotta') as color 
 UNION ALL SELECT lower('green') as color 
 UNION ALL SELECT lower('pale pink') as color 
 UNION ALL SELECT lower('green white') as color 
 UNION ALL SELECT lower('milka') as color 
 UNION ALL SELECT lower('orange pink') as color 
 UNION ALL SELECT lower('orange') as color 
 UNION ALL SELECT lower('white orange') as color 
 UNION ALL SELECT lower('yellow brown') as color 
 UNION ALL SELECT lower('red') as color 
 UNION ALL SELECT lower('cream') as color 
 UNION ALL SELECT lower('darkgreen') as color 
 UNION ALL SELECT lower('peach') as color 
 UNION ALL SELECT lower('Purple White') as color 
 UNION ALL SELECT lower('burgundy') as color 
 UNION ALL SELECT lower('Classic') as color 
 UNION ALL SELECT lower('ivory') as color 
 UNION ALL SELECT lower('light brown') as color 
 UNION ALL SELECT lower('brown') as color 
 UNION ALL SELECT lower('lilac') as color 
 UNION ALL SELECT lower('black') as color 
 UNION ALL SELECT lower('red white') as color 
 UNION ALL SELECT lower('bicolour cream-pink') as color 
 UNION ALL SELECT lower('salmon') as color 
 UNION ALL SELECT lower('dark blue') as color 
 UNION ALL SELECT lower('lime green') as color 
 UNION ALL SELECT lower('salmon pink') as color 
 UNION ALL SELECT lower('yellow orange') as color 
 UNION ALL SELECT lower('light blue pink') as color
 )

, data as(
 
select
   (SELECT color FROM color_list cl WHERE REGEXP_CONTAINS(lower(product_name), r'\b' || cl.color) limit 1) AS matched_color,
             product_name as mmmmm,

    sub_group as product_subgroup, 

--PackageLineItems
    packed_quantity,
    packages_count,
    supplied_quantity,
    pli_missing_quantity,
    pli_fulfilled_quantity,



dispatched_items,
fulfilled_items,
signed_items,
signed_status,

tags,
city,
stem_length,

number,
order_number_check,
order_id_check,

order_source,  --Direct Supplier, Express Inventory
-- order_channel,

potential_revenue,

order_id,


direct_line_order_count,
stock_line_order_count,
direct_order_ids,
stock_order_ids,


--invoices as i
    invoice_header_status, --Draft, signed, Open, Printed, Closed, Canceled, Rejected, voided

--proof_of_deliveries as pod
    proof_of_delivery_id,
    route_name,
    source_type,
    pod_status,  --DRAFT, READY, DISPATCHED, DELIVERED, SKIPPED




--actions
    dispatched_by,

--line order
    line_item_id,
    line_item_link,
    li.unit_price,
    li.total_price_without_tax, -- (li.quantity * li.unit_price)
    li.unit_landed_cost,
    
    li.unit_fob_price,
    li.packing_list_fob_price,
    li.fob_currency,

    received_fob,
    

   customer_id,

--quantity
    li.ordered_quantity,
    li.fulfilled_quantity,
    li.received_quantity,
    li.splitted_quantity,
    
    li.replaced_quantity,
    li.warehoused_quantity,

    inv_quantity, --from invoice item
    inv_total_price_without_tax,  --from invoice item

    case 
    when inv_quantity < li.fulfilled_quantity then 'Wrongly invoiced'
    when inv_quantity > li.fulfilled_quantity then 'Not invoiced'
    when inv_quantity = li.fulfilled_quantity then 'Good invoiced'
    else null end as valdiation_flag,

case 
    when li.fulfilled_quantity =0 then li.fulfilled_quantity
    when inv_quantity = 0 and li.fulfilled_quantity > 0 then li.fulfilled_quantity
    when inv_quantity is null and li.fulfilled_quantity > 0 then li.fulfilled_quantity
    when inv_quantity > li.fulfilled_quantity then li.fulfilled_quantity
    when li.fulfilled_quantity > inv_quantity then inv_quantity
    when li.fulfilled_quantity = inv_quantity then li.fulfilled_quantity
    else null
    end as the_25_aug_quantity,




--status
    li_record_type, --Purchase Order, Sale Order, To Be Scoped

    li_record_type_details,
    li.pricing_type,

    fulfillment_mode,

    order_type,                -- ONLINE, OFFLINE, ADDITIONAL, IMPORT_INVENTORY, EXTRA, RETURN, MOVEMENT
    parent_order_type,
    invoice_status,

    location as loc_status,    -- pod, loc, null

    ops_status1,               -- Received, Not Received
    ops_status2,               -- Fulfilled, Not Fulfilled
    ops_status3,               -- Prepared, Not Prepared
    ops_status4,               -- Dispatched, Not Dispatched
    ops_status5,               -- Signed, Not Signed

    state,                     --PENDING, FULFILLED, DISPATCHED, DELIVERED, CANCELED, RETURNED
    fulfillment,               --SUCCEED, PARTIAL, FAILED, UNACCOUNTED
    order_request_status,      --REQUESTED, PLACED, PARTIALLY_PLACED, REJECTED, CANCELED
    order_request_link,
    order_request_cheack,
    replaced_quantity_cheack,
    requested_quantity,
    

    Shipment,
    shipment_link,
    master_shipment_link,
    shipments_status,          --DRAFT, PACKED, WAREHOUSED, CANCELED, MISSING
    master_shipments_status,   --DRAFT, PACKED, OPENED, WAREHOUSED, CANCELED, MISSING
    master_shipment,
    order_payloads_status,   -- true, false, null
    master_shipment_id,
    shipment_id,


    creation_stage,            -- SPLIT, PACKING, INVENTORY, receiving
    ordering_stock_type,       -- INVENTORY, FLYING, null


    parent_line_item_id,
    invoice_header_id,

    invoice_number,

   



    /*
        - order placed but not received.
        - order received but not fulfilled
        - order received but not added to location in stock (for reselling purchase orders)
        - order inventory received but not picked up
        - order inventory picked put not dispatched
        - order dispatched but not delivered.
    */

    case 

        when state = 'FULFILLED' then '1.Fulfilled'
        when state = 'DISPATCHED' then '2.Dispatched'
        when state = 'DELIVERED' then '3.Delivered'
        when state = 'RETURNED' then '4.Returned'
        else '0.Not Fulfilled'
        end as order_state,

        

        li.location,

fulfillment_status,
dispatched_status,
    fulfillment_status_details,
    case when fulfillment_status_details like '%Not Fulfilled%' then 'Not Fulfilled' else 'Fulfilled' end as stage_gate2,
    case when li.dispatched_at is not null then 'Dispatched' else 'Not Dispatched' end as stage_gate3,
    
    case when fulfillment_mode not in ('Purchase Order For Inventory','Customer Sales Order From Shop') and fulfillment_status_details not in ('1. Not Fulfilled - (Investigate)','2. Fulfilled - with Full Item Incident') then pod_status else null end as pod_status2, 
    

   case 
        when fulfillment_status_details like '%Not Fulfilled%' then 'Not Fulfilled' 
        when fulfillment_status_details in ('2. Fulfilled - with Full Item Incident') then 'Fulfilled Full Incident'
        when fulfillment_status_details not like '%Not Fulfilled%' and  li.dispatched_at is null then 'Fulfilled Not Dispatched'
        when fulfillment_status_details not like '%Not Fulfilled%' and  li.dispatched_at is not null then 'Dispatched'
        else 'cheak'
        end as order_status,
        




-- fulfilled mean the item added to loc, or pod. 
    
    
    
    






internal_invoicing,



--date
    delivery_date,
    departure_date,
    dispatched_at,
  created_at as order_date,
    select_delivery_date,
    dim_date,

    signed_at, -- form ivoice level (Waqas)




CONCAT(
CASE 
WHEN EXTRACT(ISOWEEK FROM created_at) = 1 AND EXTRACT(MONTH FROM created_at) = 12 THEN CAST(EXTRACT(YEAR FROM created_at) + 1 AS STRING)
WHEN EXTRACT(ISOWEEK FROM created_at) >= 52 AND EXTRACT(MONTH FROM created_at) = 1 THEN CAST(EXTRACT(YEAR FROM created_at) - 1 AS STRING)
ELSE CAST(EXTRACT(YEAR FROM created_at) AS STRING)
END,
' - week ',
CAST(EXTRACT(ISOWEEK FROM created_at) AS STRING)
) AS Year_Week_order_at,
 


CONCAT(
CASE 
WHEN EXTRACT(ISOWEEK FROM delivery_date) = 1 AND EXTRACT(MONTH FROM delivery_date) = 12 THEN CAST(EXTRACT(YEAR FROM delivery_date) + 1 AS STRING)
WHEN EXTRACT(ISOWEEK FROM delivery_date) >= 52 AND EXTRACT(MONTH FROM delivery_date) = 1 THEN CAST(EXTRACT(YEAR FROM delivery_date) - 1 AS STRING)
ELSE CAST(EXTRACT(YEAR FROM delivery_date) AS STRING)
END,
' - week ',
CAST(EXTRACT(ISOWEEK FROM delivery_date) AS STRING)
) AS Year_Week_delivery_date,



    customer,
    debtor_number,
    account_manager,
    customer_category,
    account_type,
    customer_details,
    payment_term,
    allow_due_invoices,
    payment_term_type,

    warehouse,
    warehouse_id,
    case 
    when warehouse_id in (10,43,76,79) then 'KSA - 4 Remote Branch (HHJQ)'
    when warehouse_id in (7,9,8,6,5) then 'KSA - Main Branch'
    when warehouse_id in (1,2) then 'UAE - Kuw'
    else null 
    end as warehouse_type,

    Reseller,
    Master,

/*
case 
    when warehouse_id = 10 then 1-02-2023 -- Hail
    when warehouse_id = 43 then 1-03-2023 -- Jouf
    when warehouse_id = 76 then 10-03-2023 --Hafer
    when warehouse_id = 79 then 17-03-2023 --Qaseem
    else null
end as go_live_date,
*/




    country,
    financial_administration,
    User,
    customer_type,
    ksa_resellers,
    customer_phone_number,
    customer_email,
    

    
samer_warehouses,
    

persona,


Supplier,
-- parent_supplier,
supplier_region as Origin,
raw_supplier,

--product
    product_name as Product,
    -- product_crop as Crop,
    product_category,
    product_subcategory,

--order
    li.order_number,
    li.currency,
    li.supplier_id,












incidents_count,
    incidents_count_without_extra,
    extra_count,
    incidents_count_inventory_dmaged,
    incidents_count_without_extra_without_inventory_dmaged,

incident_cost,
    incident_cost_without_extra,
    extra_cost,
    incident_cost_inventory_dmaged,
    

incident_quantity,
    incident_quantity_without_extra,
    extra_quantity,
    incident_quantity_inventory_dmaged,


inventory_missing_quantity,
incident_quantity_receiving_stage,
incident_quantity_packing_stage,
incident_quantity_delivery_stage,
incident_quantity_inventory_stage,
incident_quantity_after_return_stage,
incident_quantity_before_supply_stage,

incident_quantity_extra_packing,
incident_quantity_extra_receiving,
incident_quantity_extra_inventory,






delivery_window,
delivery_time,

line_item_id_check,
shipment_id_check,
additional_id_check,
additional_item_link,
source_line_item_link,


invoice_id_check,
invoice_number_check,

parent_id_check,
parent_parent_id_check,
product_id_check,
source_id_check,


product_id,
product_link,
parent_line_item_link,
order_offer_id,
offer_id_check,

reseller_id_check,
customer_id_check,
supplier_id_check,

reseller_customer_id_check,
reseller_id,
customer_master_id_check,
proof_of_delivery_id_check,


-- CASE    -- Hail
--             WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and delivery_date < '2023-02-01' THEN 'regular phase'
--             WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and delivery_date >= '2023-02-01' AND delivery_date < '2023-05-01' THEN 'interim phase BMX procurement'
--             WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and delivery_date >= '2023-05-01' AND delivery_date < '2023-07-10' THEN 'interim phase FN procurement' 
--             WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and delivery_date >= '2023-07-10' THEN 'FN phase'
--         -- Jouf
--             WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and delivery_date < '2023-03-01' THEN 'regular phase'
--             WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and delivery_date >= '2023-03-01' AND delivery_date < '2023-05-01' THEN 'interim phase BMX procurement'
--             WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and delivery_date >= '2023-05-01' AND delivery_date < '2023-07-10' THEN 'interim phase FN procurement' 
--             WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and delivery_date >= '2023-07-10' THEN 'FN phase'
--          --Hafer
--             WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and delivery_date < '2023-03-10' THEN 'regular phase'
--             WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and delivery_date >= '2023-03-10' AND delivery_date < '2023-05-01' THEN 'interim phase BMX procurement'
--             WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and delivery_date >= '2023-05-01' AND delivery_date < '2023-07-10' THEN 'interim phase FN procurement' 
--             WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and delivery_date >= '2023-07-10' THEN 'FN phase'

--         --Qaseem
--             WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and delivery_date < '2023-03-17' THEN 'regular phase'
--             WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and delivery_date >= '2023-03-17' AND delivery_date < '2023-05-01' THEN 'interim phase BMX procurement'
--             WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and delivery_date >= '2023-05-01' AND delivery_date < '2023-07-10' THEN 'interim phase FN procurement' 
--             WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and delivery_date >= '2023-07-10' THEN 'FN phase'
--         ELSE 'other' 
--     END AS phase_segment,



-- CASE    -- Hail
--             WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and created_at < '2023-02-01' THEN 'regular phase'
--             WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and created_at >= '2023-02-01' AND created_at < '2023-05-01' THEN 'interim phase BMX procurement'
--             WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and created_at >= '2023-05-01' AND created_at < '2023-07-10' THEN 'interim phase FN procurement' 
--             WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and created_at >= '2023-07-10' THEN 'FN phase'
--         -- Jouf
--             WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and created_at < '2023-03-01' THEN 'regular phase'
--             WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and created_at >= '2023-03-01' AND created_at < '2023-05-01' THEN 'interim phase BMX procurement'
--             WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and created_at >= '2023-05-01' AND created_at < '2023-07-10' THEN 'interim phase FN procurement' 
--             WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and created_at >= '2023-07-10' THEN 'FN phase'
--          --Hafer
--             WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and created_at < '2023-03-10' THEN 'regular phase'
--             WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and created_at >= '2023-03-10' AND created_at < '2023-05-01' THEN 'interim phase BMX procurement'
--             WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and created_at >= '2023-05-01' AND created_at < '2023-07-10' THEN 'interim phase FN procurement' 
--             WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and created_at >= '2023-07-10' THEN 'FN phase'

--         --Qaseem
--             WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and created_at < '2023-03-17' THEN 'regular phase'
--             WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and created_at >= '2023-03-17' AND created_at < '2023-05-01' THEN 'interim phase BMX procurement'
--             WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and created_at >= '2023-05-01' AND created_at < '2023-07-10' THEN 'interim phase FN procurement' 
--             WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and created_at >= '2023-07-10' THEN 'FN phase'
--         ELSE 'other' 
--     END AS phase_segment_order_date,


--feed soure
    feed_source_name,
    feed_type,
    feed_source_supplier,

case 
    when EXTRACT(MONTH FROM delivery_date) = EXTRACT(MONTH FROM created_at) and EXTRACT(YEAR FROM delivery_date) = EXTRACT(YEAR FROM created_at) then 'Same Month Creation and Delivery'
    else 'Corner case' end as out_of_period_check,

additional_status,
additional_creation_stage,


case 
            when route_name in ('Ajman','Abu Dhabi out of City', 'Abu Dhabi City','Dubai Out of City', 'Dubai City', 'Sharjah', 'Northern Emirates', 'Ras Al Khaimah', 'Umm Al Quwain')  then 'Boxer'
            when route_name in ('Al Ain' ,'Al Ain 1', 'DXB Same Day Express', 'AUH Same Day Express') then 'Hiace'
            else null end as vehicle_type, 

    case 
            when route_name in ('Ajman','Abu Dhabi out of City', 'Abu Dhabi City','Dubai Out of City', 'Dubai City', 'Sharjah', 'Northern Emirates', 'Ras Al Khaimah', 'Umm Al Quwain')  then 11573
            when route_name in ('Al Ain' ,'Al Ain 1') then 2090

            when route_name in ('DXB Same Day Express', 'AUH Same Day Express') then 2164
            else null end as vehicle_capacity, 

vehicle_destination, 


case 
    when incidents_count is null then 'Line Item Without Incident'
    when incidents_count = extra_count and incidents_count_without_extra = 0 then 'Just Extra Incident'
    when incidents_count = incidents_count_inventory_dmaged and extra_count = 0 then 'Just Inventory Damaged Incident'
    when incidents_count = incidents_count_inventory_dmaged + extra_count then 'Just Extra & Inventory Damaged Incident'
else 'Line Item with Incident'
end as incident_detection,



case 
    when incidents_count is null then 0
    when incidents_count = extra_count and incidents_count_without_extra = 0 then 0
    when incidents_count = incidents_count_inventory_dmaged and extra_count = 0 then 0
    when incidents_count = incidents_count_inventory_dmaged + extra_count then 0
    else 1
    end as line_order_with_incidents_adjusted,


case 
    when incidents_count is null then 1
    when incidents_count = extra_count and incidents_count_without_extra = 0 then 1
    when incidents_count = incidents_count_inventory_dmaged and extra_count = 0 then 1
    when incidents_count = incidents_count_inventory_dmaged + extra_count then 1
    else 0
    end as line_order_without_incidents_adjusted,

case 
    when incidents_count is null then null
    when incidents_count = extra_count and incidents_count_without_extra = 0 then null
    when incidents_count = incidents_count_inventory_dmaged and extra_count = 0 then null
    when incidents_count = incidents_count_inventory_dmaged + extra_count then null
    else order_id
    end as order_with_incidents,


case 
    when incidents_count is null then null
    when incidents_count = extra_count and incidents_count_without_extra = 0 then null
    when incidents_count = incidents_count_inventory_dmaged and extra_count = 0 then null
    when incidents_count = incidents_count_inventory_dmaged + extra_count then null
    else proof_of_delivery_id
    end as pods_with_incidents,


-- order_with_incidents,

case when incidents_count is not null then 1 else 0 end as line_order_with_incidents,
case when incidents_count is  null then 1 else 0 end as line_order_without_incidents,


Stock,
stock_model,
stock_model_details,
full_stock_name,


invoice_header_printed_at,


case when delivery_date is not null then 'Delivery Date' else null end as delivery_date_check,
case when departure_date is not null then 'Departure Date' else null end as departure_date_check,


root_shipment_id_check,
li.source_line_item_id,


parent_product_id,


ordering_source_details,

concat(debtor_number,delivery_date) as drop_id, 

incident_orders_packing_stage,
incident_orders_receiving_stage,
incident_orders_inventory_stage,
incident_orders_delivery_stage,
incident_orders_after_return_stage,

variety_mask,
supplier_product,
case when product_color in ('colour unknown', null) then    (SELECT color
             FROM color_list cl
             WHERE REGEXP_CONTAINS(lower(product_name), r'\b' || cl.color)
             limit 1) else product_color end as product_color,

current_timestamp() as insertion_timestamp, 

order_request_id,
import_sheet_id,
source_warehouse,
destination_warehouse,
created_by,

auto_gross_revenue,
auto_credit_note,
total_cost,
gross_revenue,
credit_note,
delivery_charge_amount,
unit_landed_cost * ordered_quantity as total_cost_mod,

li_invoice_number,
sales_unit,
local_supplier_name,
CASE
    WHEN warehouse IN ('Dammam Project X', 'Dammam Warehouse', 'Hafar WareHouse') THEN 'Dammam'
    WHEN warehouse = 'Dubai Warehouse' THEN 'Dubai'
    WHEN warehouse IN ('Riyadh Warehouse', 'Riyadh Project X', 'Qassim Warehouse', 'Hail Warehouse') THEN 'Riyadh'
    WHEN warehouse IN ('Jeddah Warehouse', 'Tabuk Warehouse', 'Medina Warehouse', 'Jouf WareHouse', 'Jeddah Project X') THEN 'Jeddah'
    WHEN warehouse = 'Qatar Warehouse' THEN 'Qatar'
    WHEN warehouse = 'Kuwait Warehouse' THEN 'Kuwait'
    WHEN warehouse = 'Jordan Warehouse' THEN 'Jordan'
    ELSE warehouse
END AS main_hub,

selling_stage,

total_quantity,
damaged_packing_quantity,
sh_received_quantity,
sh_requested_quantity,
sh_packed_quantity,

missing_packing_quantity,
sh_incident_quantity_receiving_stage,
missing_quantity_receiving_stage,
damaged_quantity_receiving_stage,
extra_quantity_receiving_stage,
packing_additional_quantity,
extra_packing_quantity,

from {{ref('int_line_items')}} as li 

)
select *,    
CONCAT(coalesce(product_subcategory,''), coalesce(product_subgroup,''), coalesce(product_color,'')) as li_category_linking,
CONCAT(coalesce(product,''), coalesce(product_color,'')) as li_product_linking
from data 

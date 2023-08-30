with

source as ( 

 
select 

--actions
    dispatched_by,

--line order
    line_item_id,
    line_item_link,
    li.unit_price,
    li.total_price_without_tax, -- (li.quantity * li.unit_price)
    li.unit_landed_cost,

    customer_id,

--quantity
    li.ordered_quantity,
    li.fulfilled_quantity,
    li.received_quantity,

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
    record_type,               -- Purchase Order, Customer Order, System
    record_type_details,       -- Reseller Purchase Order, Customer Bulk Order, Customer Shipment Order, Customer Inventory Order, Customer Fly Order, stock2stock, EXTRA, RETURN, MOVEMENT
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
    pod_status,                --DRAFT, READY, DISPATCHED, DELIVERED, SKIPPED
    order_request_status,      --REQUESTED, PLACED, PARTIALLY_PLACED, REJECTED, CANCELED
    Shipment,
    shipment_link,
    master_shipment_link,
    shipments_status,          --DRAFT, PACKED, WAREHOUSED, CANCELED, MISSING
    master_shipments_status,   --DRAFT, PACKED, OPENED, WAREHOUSED, CANCELED, MISSING
    master_shipment_name as master_shipment,
    order_payloads_status,     -- true, false, null


    creation_stage,            -- SPLIT, PACKING, INVENTORY, receiving
    ordering_stock_type,       -- INVENTORY, FLYING, null


    parent_line_item_id,
    invoice_header_id,

    

   



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

        fulfillment_mode,

        li.location,


    fulfillment_status,
    case when fulfillment_status like '%Not Fulfilled%' then 'Not Fulfilled' else 'Fulfilled' end as stage_gate2,
    case when li.dispatched_at is not null then 'Dispatched' else 'Not Dispatched' end as stage_gate3,
    
    case when fulfillment_mode not in ('Reselling Orders (Stock-in)','Customer In Shop Order') and fulfillment_status not in ('1. Not Fulfilled - (Investigate)','2. Fulfilled - with Full Item Incident') then pod_status else null end as pod_status2, 
    

   case 
        when fulfillment_status like '%Not Fulfilled%' then 'Not Fulfilled' 
        when fulfillment_status in ('2. Fulfilled - with Full Item Incident') then 'Fulfilled Full Incident'
        when fulfillment_status not like '%Not Fulfilled%' and  li.dispatched_at is null then 'Fulfilled Not Dispatched'
        when fulfillment_status not like '%Not Fulfilled%' and  li.dispatched_at is not null then 'Dispatched'
        else 'cheak'
        end as order_status,
        




--fulfilled mean the item added to loc, or pod. 
    
    
    
    






internal_invoicing,



--date
    delivery_date,
    departure_date,
    dispatched_at,
    created_at as order_date,
    select_delivery_date,
    dim_date,


--Customer
    Customer,
    debtor_number,
    account_manager,
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

--pod
    proof_of_delivery_id,
    
    
    source_type,

    
samer_warehouses,
    




Supplier,
parent_supplier,
supplier_region as Origin,

--product
    product_name as Product,
    product_crop as Crop,
    product_category,
    product_subcategory,

--order
    li.order_number,
    li.currency,
    li.supplier_id,












incidents_count,
incident_quantity,
inventory_missing_quantity,
incident_quantity_receiving_stage,
incident_quantity_packing_stage,
incident_quantity_extra,
incident_quantity_inventory_stage,

incident_quantity_extra_packing,
incident_quantity_extra_receiving,
incident_quantity_extra_inventory,



delivery_window,
delivery_time,


shipment_id_check,
invoice_id_check,
parent_id_check,
product_id_check,
product_id,
product_link,
parent_line_item_link,
offer_id,
offer_id_check,
reseller_id_check,
reseller_id,
customer_master_id_check,
proof_of_delivery_id_check,


CASE    -- Hail
            WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and delivery_date < '2023-02-01' THEN 'regular phase'
            WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and delivery_date >= '2023-02-01' AND delivery_date < '2023-05-01' THEN 'interim phase BMX procurement'
            WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and delivery_date >= '2023-05-01' AND delivery_date < '2023-07-10' THEN 'interim phase FN procurement' 
            WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and delivery_date >= '2023-07-10' THEN 'FN phase'
        -- Jouf
            WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and delivery_date < '2023-03-01' THEN 'regular phase'
            WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and delivery_date >= '2023-03-01' AND delivery_date < '2023-05-01' THEN 'interim phase BMX procurement'
            WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and delivery_date >= '2023-05-01' AND delivery_date < '2023-07-10' THEN 'interim phase FN procurement' 
            WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and delivery_date >= '2023-07-10' THEN 'FN phase'
         --Hafer
            WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and delivery_date < '2023-03-10' THEN 'regular phase'
            WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and delivery_date >= '2023-03-10' AND delivery_date < '2023-05-01' THEN 'interim phase BMX procurement'
            WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and delivery_date >= '2023-05-01' AND delivery_date < '2023-07-10' THEN 'interim phase FN procurement' 
            WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and delivery_date >= '2023-07-10' THEN 'FN phase'

        --Qaseem
            WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and delivery_date < '2023-03-17' THEN 'regular phase'
            WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and delivery_date >= '2023-03-17' AND delivery_date < '2023-05-01' THEN 'interim phase BMX procurement'
            WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and delivery_date >= '2023-05-01' AND delivery_date < '2023-07-10' THEN 'interim phase FN procurement' 
            WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and delivery_date >= '2023-07-10' THEN 'FN phase'
        ELSE 'other' 
    END AS phase_segment,



CASE    -- Hail
            WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and created_at < '2023-02-01' THEN 'regular phase'
            WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and created_at >= '2023-02-01' AND created_at < '2023-05-01' THEN 'interim phase BMX procurement'
            WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and created_at >= '2023-05-01' AND created_at < '2023-07-10' THEN 'interim phase FN procurement' 
            WHEN debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') and created_at >= '2023-07-10' THEN 'FN phase'
        -- Jouf
            WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and created_at < '2023-03-01' THEN 'regular phase'
            WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and created_at >= '2023-03-01' AND created_at < '2023-05-01' THEN 'interim phase BMX procurement'
            WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and created_at >= '2023-05-01' AND created_at < '2023-07-10' THEN 'interim phase FN procurement' 
            WHEN debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') and created_at >= '2023-07-10' THEN 'FN phase'
         --Hafer
            WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and created_at < '2023-03-10' THEN 'regular phase'
            WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and created_at >= '2023-03-10' AND created_at < '2023-05-01' THEN 'interim phase BMX procurement'
            WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and created_at >= '2023-05-01' AND created_at < '2023-07-10' THEN 'interim phase FN procurement' 
            WHEN debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') and created_at >= '2023-07-10' THEN 'FN phase'

        --Qaseem
            WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and created_at < '2023-03-17' THEN 'regular phase'
            WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and created_at >= '2023-03-17' AND created_at < '2023-05-01' THEN 'interim phase BMX procurement'
            WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and created_at >= '2023-05-01' AND created_at < '2023-07-10' THEN 'interim phase FN procurement' 
            WHEN debtor_number in ('130257','LNDQAS','EVEQAS','ASTQAS') and created_at >= '2023-07-10' THEN 'FN phase'
        ELSE 'other' 
    END AS phase_segment_order_date,


--feed soure
    feed_source_name,
    feed_type,
    feed_source_supplier,

case 
    when EXTRACT(MONTH FROM delivery_date) = EXTRACT(MONTH FROM created_at) and EXTRACT(YEAR FROM delivery_date) = EXTRACT(YEAR FROM created_at) then 'Same Month Creation and Delivery'
    else 'Corner case' end as out_of_period_check,

additional_status,
additional_creation_stage,

current_timestamp() as insertion_timestamp, 


from {{ref('int_line_items')}} as li 
)

select * from source
with

source as ( 

 
select 

--line order
    line_item_id,
    line_item_link,
    li.unit_price,
    li.total_price_without_tax, -- (li.quantity * li.unit_price)

--quantity
    li.quantity,
    li.fulfilled_quantity,



--status
    record_type,               -- Purchase Order, Customer Order, System
    record_type_details,       -- Reseller Purchase Order, Customer Bulk Order, Customer Shipment Order, Customer Inventory Order, Customer Fly Order, stock2stock, EXTRA, RETURN, MOVEMENT
    order_type,                -- ONLINE, OFFLINE, ADDITIONAL, IMPORT_INVENTORY, EXTRA, RETURN, MOVEMENT
    parent_order_type,

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
    shipments_status,          --DRAFT, PACKED, WAREHOUSED, CANCELED, MISSING
    master_shipments_status,   --DRAFT, PACKED, OPENED, WAREHOUSED, CANCELED, MISSING
    order_payloads_status,     -- true, false, null


    creation_stage,            -- SPLIT, PACKING, INVENTORY, receiving
    ordering_stock_type,       -- INVENTORY, FLYING, null

    fulfillment_status,



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
    
    
--fulfilled mean the item added to loc, or pod. 
    
    
    
    






internal_invoicing,



--date
    delivery_date,
    departure_date,
    created_at as order_date,
    select_delivery_date,
    dim_date,


--Customer
    Customer,
    debtor_number,
    account_manager,
    warehouse,
    country,
    financial_administration,
    User,

--pod
    proof_of_delivery_id,
    
    dispatched_by,
    source_type,

    

    




Supplier,
supplier_region as Origin,

--product
    product_name as Product,
    product_crop as Crop,
    product_category,
    product_subcategory,

--order
    li.order_number,
    li.currency,












li.incidents_count,



current_timestamp() as insertion_timestamp, 


from {{ref('int_line_items')}} as li 
)

select * from source
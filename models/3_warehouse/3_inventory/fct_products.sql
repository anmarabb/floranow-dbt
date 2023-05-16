-- fullfeled (added to loc, and genrate product_location recourd)
--

with

source as ( 

 
select 
    product_id,
    product_link,
    record_type,
    record_type_details,

    Supplier,
    product_name as Product,
    Reseller,
    Stock,
    full_stock_name,
    loc_status,

    live_stock,


    
    order_status,
    fulfillment_status,
    fulfillment_mode,
    warehouse,
    fulfillment,


--fct
    ordered_quantity,
    
    published_quantity,
    remaining_quantity,

    location_quantity,
    location_remaining_quantity,

    fulfilled_quantity,
    sold_quantity,
    incidents_quantity,
    damaged_quantity,


    

    User,
    order_type,

    departure_date,   --from product
    delivery_date,    --from line item
    select_delivery_date,
    select_departure_date,
    calc_select_departure_date,
    calc_departure_date, --order_date for import inventory
    calc_delivery_date, --order_date for import inventory


    age,
    Visibility,

    full_incident_check,

    Shipment,
    shipments_status,
    master_shipments_status,

current_timestamp() as insertion_timestamp, 


from {{ref('int_products')}} as p 
)

select * from source


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


    
    order_status,
    fulfillment_status,
    fulfillment_mode,
    warehouse,

    ordered_quantity,
    location_quantity,
    fulfilled_quantity,
    selled_quantity,
    incidents_quantity,
    damaged_quantity,


    delivery_date,

    User,
    order_type,
    select_delivery_date,

    age,
    Visibility,

current_timestamp() as insertion_timestamp, 


from {{ref('int_products')}} as p 
)

select * from source


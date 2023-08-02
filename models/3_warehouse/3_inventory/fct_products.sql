-- fullfeled (added to loc, and genrate product_location recourd)
--

with

source as ( 

 
select 

--Products
    --dim
        product_name as Product,
        product_crop,
        product_category,
        new_category,


        Supplier,
        Origin,
        Reseller,
        Stock,
        full_stock_name,
        live_stock,
        stock_model,
        stock_model_details,
        Visibility,

        product_id,
        product_link,
    

    --date
        expired_at,
        departure_date,   --from product
        order_date,
  
    --fct
        
        remaining_quantity,
        published_quantity,
        remaining_value,
        landed_remaining_value,
        age,
        fulfilled_quantity,

        unit_landed_cost,
        unit_price,
        unit_fob_price,

        out_feed_source_name,
    



--product_locations
    --dim

    --date
  
    --fct
        location_quantity,
        location_remaining_quantity,




--line_items
    --dim
        record_type,
        record_type_details,
        fulfillment,
        fulfillment_status,
        fulfillment_mode,
        User,
        loc_status,
        order_status,
        order_type,
        warehouse,
        Shipment,
        shipments_status,
        master_shipments_status,
        master_shipment,

    --date
        delivery_date,    --from line item
  
    --fct
    
    ordered_quantity,
    received_quantity,

    inventory_product_quantity,
    
    
    

    
--line_items_sold
    sold_quantity,
    child_incident_quantity,
    

--product_incidents
    incidents_quantity,
    incidents_quantity_location,
    cleanup_adjustments_quantity,
    toat_damaged_quantity,
    inventory_damaged_quantity,
    extra_quantity,
    inventory_extra_quantity,
    packing_extra_quantity,







    select_delivery_date,
    select_departure_date,


    
    

    full_incident_check,


current_timestamp() as insertion_timestamp, 


from {{ref('int_products')}} as p 
)

select * from source


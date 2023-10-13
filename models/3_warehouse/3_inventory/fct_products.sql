            
            
            
            WITH future_orders as (
                WITH CTE AS (
                SELECT 
                    departure_date,
                    warehouse,
                    product_id,
                    --product_name,
                    DENSE_RANK() OVER (PARTITION BY warehouse,product_name ORDER BY departure_date) AS departure_rank,
                from {{ ref('int_products')}} as p
                WHERE departure_date >= CURRENT_DATE() --and product_name like '%Rose Athena%' and warehouse ='Riyadh Warehouse'

                            )
                SELECT 
                *,
                case 
                    when departure_rank = 1 then 'first_departure'
                    when departure_rank = 2 then 'second_departure'
                    else null end as departure_ranking
                FROM CTE

               --where product_name like '%Rose Ever Red%' and warehouse ='Riyadh Warehouse'
                --where product_id=157823
            )
                
            

 
select 

--Products
    --dim
        product_name as Product,
        stem_length,
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
        shipment_link,

        p.product_id,
        product_link,

        flag_1,

     
    

    --date
        expired_at,
        expired_at_2,
        p.departure_date,   --from product
        order_date,
  
    --fct
        
        remaining_quantity,
        case when flag_1 != 'not_scaned'  and live_stock = 'Live Stock' and Stock = 'Inventory Stock' then remaining_quantity else 0 end as in_stock_quantity,
        case when select_departure_date in ('Future', 'Today') then ordered_quantity else 0 end as coming_quantity,
        --case when select_departure_date in ('Future', 'Today') then MIN(departure_date) else null end AS next_departure_date,



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
        p.warehouse,
        Shipment,
        shipments_status,
        master_shipments_status,
        master_shipment,

        route_name,

    --date
        delivery_date,    --from line item
  
    --fct
    
    ordered_quantity,
        last_30d_ordered_quantity,
    received_quantity,

    inventory_product_quantity,
    
    
    

    
--line_items_sold
    sold_quantity,
        last_30d_sold_quantity,
    child_incident_quantity,
    

--product_incidents
    incidents_quantity,
        last_30d_incidents_quantity,

    inventory_damaged_quantity,
        last_30d_inventory_damaged_quantity,


    incidents_quantity_location,
    cleanup_adjustments_quantity,
    toat_damaged_quantity,
    
    extra_quantity,
    inventory_extra_quantity,
    packing_extra_quantity,







    select_delivery_date,
    select_departure_date,


    
    

    full_incident_check,
    sku,


fo.departure_ranking,

case when fo.departure_ranking ='first_departure' then ordered_quantity else 0 end as first_departure_coming_quantity,
case when fo.departure_ranking ='second_departure' then ordered_quantity else 0 end as second_departure_coming_quantity,

case 
when case when flag_1 != 'not_scaned'  and live_stock = 'Live Stock' and Stock = 'Inventory Stock' then remaining_quantity else 0 end = 0 and last_30d_sold_quantity = 0 and last_30d_inventory_damaged_quantity = 0 and case when select_departure_date in ('Future', 'Today') then ordered_quantity else 0 end = 0 then 'Outdated Products'
else 'Active Products'
end as product_activity_status,


current_timestamp() as insertion_timestamp, 


from {{ref('int_products')}} as p 
left join future_orders as fo on fo.product_id = p.product_id

--where p.product_id = 157823
--where product_name like '%Rose Athena%' and p.warehouse ='Riyadh Warehouse' and select_departure_date in ('Future', 'Today')
--Rose Athena
            
            
            
            WITH future_orders as (
                WITH CTE AS (
                SELECT 
                    departure_date,
                    warehouse,
                    product_id,
                    DENSE_RANK() OVER (PARTITION BY warehouse ORDER BY departure_date) AS departure_rank
                from {{ ref('int_products')}} as p
                WHERE departure_date >= CURRENT_DATE()

                            )
                SELECT 
                *,
                case 
                    when departure_rank = 1 then 'first_departure'
                    when departure_rank = 2 then 'second_departure'
                    else null end as departure_ranking
                FROM CTE
            )
                
            

 
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

        p.product_id,
        product_link,
    

    --date
        expired_at,
        p.departure_date,   --from product
        order_date,
  
    --fct
        
        remaining_quantity,
        case when stock_model = 'Reselling' and live_stock = 'Live Stock' and Stock = 'Inventory Stock' then remaining_quantity else 0 end as in_stock_quantity,
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
    sku,


fo.departure_ranking,

case when fo.departure_ranking ='first_departure' then ordered_quantity else 0 end as frirst_departure_coming_quantity,
case when fo.departure_ranking ='second_departure' then ordered_quantity else 0 end as second_departure_coming_quantity,


current_timestamp() as insertion_timestamp, 


from {{ref('int_products')}} as p 
left join future_orders as fo on fo.product_id = p.product_id
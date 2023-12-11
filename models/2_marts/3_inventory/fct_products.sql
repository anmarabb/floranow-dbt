            
            
            
            WITH future_orders as (
                WITH CTE AS (
                SELECT 
                    departure_date,
                    warehouse,
                    product_id,
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
                

            )
                
            

 
select 


--Products
    --dim
        product_name as Product,
        stem_length,
        product_subcategory,
        product_category,


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
        line_item_link,

        flag_1,

        
multi_location,
     
    

    --date
        product_expired_at,
        p.departure_date,   --from product
        order_date,
  
    --fct
        
        remaining_quantity,
        case when flag_1 != 'not_scaned'  and live_stock = 'Live Stock' and Stock = 'Inventory Stock' then remaining_quantity else 0 end as in_stock_quantity,
        case when select_departure_date in ('Future', 'Today') then ordered_quantity else 0 end as coming_quantity,
        case when select_departure_date not in ('Future', 'Today') then ordered_quantity else 0 end as past_ordered_quantity,

        case when select_departure_date in ('last_10_days') and loc_status is null and shipments_status != 'DRAFT' and order_status != 'Fulfilled Full Incident' then ordered_quantity else 0 end as transit_quantity,

        --case when select_departure_date in ('Future', 'Today') then MIN(departure_date) else null end AS next_departure_date,



        published_quantity,
        remaining_value,
        landed_remaining_value,
        age,

        
        fulfilled_quantity,
        packed_quantity,

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
        li_record_type,
        li_record_type_details,
        fulfillment,
        fulfillment_status,
        fulfillment_status_details,
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
    requested_quantity,
    ordered_quantity,
        last_30d_ordered_quantity,
    received_quantity,

    inventory_product_quantity,
    
    
    

    
--line_items_sold
    sold_quantity,
        last_30d_sold_quantity,
    child_incident_quantity,
    item_sold,
    customer_ordered,
    

--product_incidents
    incidents_quantity,
        last_30d_incidents_quantity,

    incident_quantity_inventory_dmaged,
    incident_quantity_inventory_stage,
        last_30d_incident_quantity_inventory_dmaged,


    incidents_quantity_location,
    cleanup_adjustments_quantity,
    toat_damaged_quantity,

    incident_quantity_packing_stage,
    incident_quantity_receiving_stage,
    
    extra_quantity,
    inventory_extra_quantity,
    packing_extra_quantity,







    select_departure_date,

    
    

    full_incident_check,
    sku,


fo.departure_ranking,

case when fo.departure_ranking ='first_departure' then ordered_quantity else 0 end as first_departure_coming_quantity,
case when fo.departure_ranking ='second_departure' then ordered_quantity else 0 end as second_departure_coming_quantity,

case when fo.departure_ranking ='first_departure' then p.departure_date else null end as first_departure_date,
case when fo.departure_ranking ='second_departure' then p.departure_date else null end as second_departure_date,

case 
when case when flag_1 != 'not_scaned'  and live_stock = 'Live Stock' and Stock = 'Inventory Stock' then remaining_quantity else 0 end = 0 and last_30d_sold_quantity = 0 and last_30d_incident_quantity_inventory_dmaged = 0 and case when select_departure_date in ('Future', 'Today') then ordered_quantity else 0 end = 0 then 'Outdated Products'
else 'Active Products'
end as product_activity_status,


Location,
p.modified_expired_at,
DATE_DIFF(p.modified_expired_at, p.departure_date, DAY) AS difference_in_days,
shelf_life_days,



case when line_item_id is not null then 'Line Item ID' else null end as line_item_id_check,
case when p.delivery_date is not null then 'Delivery Date' else null end as delivery_date_check,
case when p.departure_date is not null then 'Departure Date' else null end as departure_date_check,

created_at_check,
additional_items_check,
inventory_item_type,

product_created_at,
order_source,
persona,
line_item_id,
source_line_item_id,

parent_parent_id_check,
parent_id_check,


DATE_DIFF(date(p.departure_date), date(p.order_date), DAY) AS lead_time,
PARSE_DATE('%Y-%m-%d', CONCAT(FORMAT_TIMESTAMP('%Y-%m', p.departure_date), '-01')) as year_month_departure_date,



CASE 
        WHEN EXTRACT(YEAR FROM p.departure_date) = 2022 THEN sold_quantity 
        ELSE 0 
    END AS sold_quantity_2022,
CASE 
        WHEN EXTRACT(YEAR FROM p.departure_date) = 2023 THEN sold_quantity 
        ELSE 0 
    END AS sold_quantity_2023,


case when COALESCE(incidents_quantity, 0) + COALESCE(fulfilled_quantity, 0)  = ordered_quantity then 'Match' else 'Cheack' end as quantity_cheack,


master_shipment_id,
shipment_id,


DATE_DIFF(modified_expired_at, CURRENT_DATE(), DAY) AS days_until_expiry,




STDDEV_POP(sold_quantity) over (partition by p.product_name, p.warehouse) AS sold_quantity_stddev,


--max(case when fo.departure_ranking ='first_departure' then p.departure_date else null end) over (partition by p.product_name, p.warehouse) as first_departure_date,


current_timestamp() as insertion_timestamp, 

from {{ref('int_products')}} as p 
left join future_orders as fo on fo.product_id = p.product_id

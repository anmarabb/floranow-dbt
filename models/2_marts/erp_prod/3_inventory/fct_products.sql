            
            
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
                

            ),

             requested_orders as(
                with data as (
                select 
                    p.product_id,
                    p.Product,
                    p.warehouse,
                    li.departure_date,
                    --orr.id,
                    sum(li.requested_quantity) as requested_quantity,
                    DENSE_RANK() OVER (PARTITION BY p.warehouse,p.Product ORDER BY li.departure_date) AS departure_rank,
                    --case when DENSE_RANK() OVER (PARTITION BY p.warehouse,p.Product ORDER BY orr.departure_date) = 1 then "right" end as norhan
                from `dbt_prod_dwh.fct_products` p
                left join `dbt_prod_dwh.fct_order_items` li on p.line_item_id = li.line_item_id
                -- left join `dbt_prod_stg.stg_order_requests` orr on orr.id = li.order_request_id
                WHERE li.departure_date >= CURRENT_DATE() -- and p.Product = 'Alstroemeria Elegance'  --and orr.departure_date is not null
                group by 1, 2, 3, 4--, 5
                            )
                select *,
                case 
                    when departure_rank = 1 then 'first_departure'
                    when departure_rank = 2 then 'second_departure'
                    else null end as departure_request_rank,
          
                FROM data

            ),
            flags as (
  select product_name,
         departure_date,
         sum(in_stock_quantity) as daily_qoh,
         first_value(sum(in_stock_quantity)) over (partition by product_name order by departure_date rows between unbounded preceding and unbounded following) as first_batch_qoh,
         max(sum(in_stock_quantity)) over (partition by product_name order by departure_date rows between 1 following and unbounded following) as max_future_qoh,
         case when max(sum(in_stock_quantity)) over (partition by product_name order by departure_date rows between 1 following and unbounded following) is null then 0
              when first_value(sum(in_stock_quantity)) over (partition by product_name order by departure_date rows between unbounded preceding and unbounded following) > max(sum(in_stock_quantity)) 
              over (partition by product_name order by departure_date rows between 1 following and unbounded following) then 1 else 0
         end as fifo_flag

  from {{ref("int_products")}}
  group by product_name, departure_date
),
express_data as (
    select p.product_id, 
           date(cli.created_at) as order_date,
           li.fulfilled_quantity, 
           cli.ordered_quantity as withdrown_quantity,
    from {{ref("int_products")}} p
    left join {{ref ("base_stocks")}} s on p.stock_id = s.stock_id
    left join {{ref ("int_line_items")}} li on li.line_item_id = p.line_item_id
    left join{{ref ("int_line_items")}} cli on cli.parent_line_item_id = li.line_item_id and cli.customer_type != 'retail'
    left join {{ref("stg_feed_sources")}} fs on cli.feed_source_id = fs.feed_source_id 
    where s.out_feed_source_id in (277, 271, 578, 991, 445, 886, 1025, 683, 615, 986, 614, 887, 545, 990, 443, 989, 987, 988, 1026)

)
                
            

 
select 

case 
  when Stock = 'Inventory Stock' and live_stock = 'Live Stock' and stock_model in ('Reselling', 'Commission Based') and flag_1 in ('scaned_flag', 'scaned_good') then 'Current Inventory' 
  when Stock = 'Inventory Stock' and live_stock = 'Live Stock' and stock_model in ('Internal', 'Internal - Project X') and flag_1 in ('scaned_flag', 'scaned_good') then 'Internal - Current Inventory'
  else null 
  end as report_filter,


case 
  when Stock = 'Inventory Stock' and live_stock = 'Live Stock' and stock_model in ('Reselling', 'Commission Based') and flag_1 in ('scaned_flag', 'scaned_good') then in_stock_quantity 
  end as current_in_stock_quantity,

--Products
    --dim
        p.product_name as Product,
        stem_length,
        product_subcategory,
        product_category,


        Supplier,
      --  orginal_supplier,
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

        master_shipment_id,

        flag_1,
        flag,

        stock_id,
     
    

    --date
        product_expired_at,
        p.departure_date,   --from product
        p.order_date,
  
    --fct
        
        remaining_quantity,
        --case when flag_1 != 'not_scaned'  and live_stock = 'Live Stock' and Stock = 'Inventory Stock' then remaining_quantity else 0 end as in_stock_quantity,
        in_stock_quantity,
        
        --case when select_departure_date in ('Future', 'Today') then ordered_quantity else 0 end as coming_quantity,
        case when fulfillment in ('UNACCOUNTED') AND p.departure_date >= CURRENT_DATE then ordered_quantity else 0 end as coming_quantity,




        case when select_departure_date not in ('Future', 'Today') then ordered_quantity else 0 end as past_ordered_quantity,

        case when select_departure_date = 'last_10_days'  and  shipments_status != 'DRAFT' and order_status != 'Fulfilled Full Incident' and loc_status is null then p.ordered_quantity else 0 end as transit_quantity_awais,
        -- case when select_departure_date = 'last_10_days'  and  shipments_status != 'DRAFT' and order_status != 'Fulfilled Full Incident' and loc_status is null then p.remaining_quantity else 0 end as transit_quantity,

        
        case when (select_departure_date = 'last_10_days' and (loc_status is null or loc_status = 'null')) and ( shipments_status != 'DRAFT' and order_status != 'Fulfilled Full Incident') then p.ordered_quantity else 0 end as transit_quantity,

        --case when select_departure_date in ('Future', 'Today') then MIN(departure_date) else null end AS next_departure_date,



        published_quantity,
        remaining_value,
        landed_remaining_value,
        age,

        
        p.fulfilled_quantity,
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
        location_count,




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
    p.requested_quantity,
    ordered_quantity,
        last_30d_ordered_quantity,
    received_quantity,

    inventory_product_quantity,
    
    
    

    
--line_items_sold
    sold_quantity,
    last_30d_sold_quantity,
    last_7d_sold_quantity,
    last_year_30d_sold_quantity,
    last_year_7d_sold_quantity,
    last_year_next_7d_sold_quantity,
    child_incident_quantity,
    item_sold,
    customer_ordered,


--line_items_inv_sold
    i_sold_quantity,
    i_last_30d_sold_quantity,
    i_last_7d_sold_quantity,
    i_last_year_30d_sold_quantity,
    i_last_year_7d_sold_quantity,
    i_last_year_next_7d_sold_quantity,
    

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
    COALESCE(inventory_extra_quantity,0) as inventory_extra_quantity,



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


--Location,
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
-- persona,
line_item_id,
source_line_item_id,

parent_parent_id_check,
parent_id_check,


DATE_DIFF(date(p.departure_date), date(p.order_date), DAY) AS lead_time,
case when extract(year from p.departure_date) = 2023 then DATE_DIFF(date(p.departure_date), date(p.order_date), DAY) end AS lead_time_2023,

PARSE_DATE('%Y-%m-%d', CONCAT(FORMAT_TIMESTAMP('%Y-%m', p.departure_date), '-01')) as year_month_departure_date,



CASE 
        WHEN EXTRACT(YEAR FROM p.departure_date) = 2022 THEN sold_quantity 
        ELSE 0 
    END AS sold_quantity_2022,
CASE 
        WHEN EXTRACT(YEAR FROM p.departure_date) = 2023 THEN sold_quantity 
        ELSE 0 
    END AS sold_quantity_2023,


case when COALESCE(incidents_quantity, 0) + COALESCE(p.fulfilled_quantity, 0)  = ordered_quantity then 'Match' else 'Cheack' end as quantity_cheack,


shipment_id,

DATE_DIFF(modified_expired_at, CURRENT_DATE(), DAY) AS days_until_expiry,

case when DATE_DIFF(modified_expired_at, CURRENT_DATE(), DAY) <=0 then 0 else in_stock_quantity end as active_in_stock_quantity,
case when DATE_DIFF(modified_expired_at, CURRENT_DATE(), DAY) <=2 then 0 else in_stock_quantity end as stable_in_stock_quantity,

case when DATE_DIFF(modified_expired_at, CURRENT_DATE(), DAY) <0 then in_stock_quantity else 0 end as expired_stock_quantity,

case when DATE_DIFF(modified_expired_at, CURRENT_DATE(), DAY) in (0,1,2) then in_stock_quantity else 0 end as aging_stock_quantity,

case 
 when DATE_DIFF(modified_expired_at, CURRENT_DATE(), DAY) <0 then 'Expired Stock'
 when DATE_DIFF(modified_expired_at, CURRENT_DATE(), DAY) in (0,1,2) then 'Expiring Soon Stock'
 when DATE_DIFF(modified_expired_at, CURRENT_DATE(), DAY) >2 then 'Stable Stock'
 else 'Check'
 end as inventory_status,


STDDEV_POP(sold_quantity) over (partition by p.product_name, p.warehouse) AS sold_quantity_stddev,


--max(case when fo.departure_ranking ='first_departure' then p.departure_date else null end) over (partition by p.product_name, p.warehouse) as first_departure_date,
case
when master_shipments_status in ('DRAFT' ) then '1. Not Received - Draft'
when master_shipments_status in ( 'PACKED' ) then '2. Not Received - Packed (Comming Soon)'
when master_shipments_status  in ( 'OPENED' ) then '3. Received - Work In Progress'
when master_shipments_status  in ('WAREHOUSED' ) then '4. Received - Work Done'
else '5. Not Shipment'
end as shipment_progress,

case 
    when loc_status = 'null' and master_shipments_status in ('DRAFT') then '1. Not Received (Draft Master Shipments)'
    when loc_status = 'null' and master_shipments_status in ('PACKED') and shipments_status not in ('MISSING') then '2. Not Received (Packed Master Shipments)'
    when loc_status = 'null' and shipments_status in ('MISSING') then '3. Not Received (Full Missing Shipments)'
    
    when loc_status = 'null' and master_shipments_status in ('OPENED') and shipments_status in ('PACKED') and  order_status = 'Not Fulfilled' then '4. Received Not Scanned (Work in Progress)'

    when loc_status = 'null' and shipments_status in ('PACKED', 'WAREHOUSED') and fulfillment_status_details = '2. Fulfilled - with Full Item Incident' then '5. Received Full Incident'
    when loc_status = 'null' and fulfillment_status_details = '3. Fulfilled - with Process Breakdown' then '6. Received and Fulfilled without Scanned to Location'
    when loc_status = 'loc'  then '7. Received On Location'
    else '8. To Be Scoped'
    end as orders_progress, 




ordering_stock_type,
line_item_state,
online_item,
stock_type,
current_timestamp() as insertion_timestamp, 
number,
financial_administration,

case when ro.departure_request_rank ='first_departure' then ro.requested_quantity else 0 end as first_departure_requested_quantity,
case when ro.departure_request_rank ='first_departure' then ro.departure_date else null end as first_request_departure_date,
product_color,

missing_packing_quantity,
incident_quantity_receiving_stage_with_extra,
missing_quantity_receiving_stage,
damaged_quantity_receiving_stage,
extra_quantity_receiving_stage,
total_quantity,
pli_packed_quantity,
li_extra_packing_quantity,
packing_additional_quantity,
pli_received_quantity,

case 
    when reseller = 'Dammam Event' then 'Dammam Warehouse'
    when reseller = 'Floranow UAE Event' then 'Dubai Warehouse'
    when reseller = 'Riyadh Event' then 'Riyadh Warehouse'
    when reseller = 'Tabuk Event' then 'Tabuk Warehouse'
    else p.warehouse end as warehouse_modified,

sales_unit,
li_invoice_number,
local_supplier_name,
total_price_without_tax,
currency,
packing_list_fob_price,

modified_stock_model,
modified_stock_model_details,

f.fifo_flag,

ed.order_date as express_order_date,
ed.fulfilled_quantity as total_fulfilled_quantity_express, 
ed.withdrown_quantity as withdrown_quantity_express,

from {{ref('int_products')}} as p 
left join future_orders as fo on fo.product_id = p.product_id
left join requested_orders as ro on ro.product_id = p.product_id
left join flags f on p.product_name = f.product_name and p.departure_date = f.departure_date
left join express_data ed on ed.product_id = p.product_id
--where p.product_id = 268380
--where shipment_id =30798
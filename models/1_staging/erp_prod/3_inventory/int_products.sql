with CTE as 

    (
        with 
            product_incidents as (
                select 
                p.product_id,
                    count(*) as incidents_count,
                    --sum(pi.quantity) as incidents_quantity,
                    sum(case when incident_type !='EXTRA' and after_sold is false then pi.quantity else 0 end) as incidents_quantity,
                        SUM(CASE WHEN DATE_DIFF(CURRENT_DATE(), date(pi.incident_at), DAY) <= 30 AND incident_type != 'EXTRA' AND after_sold = false THEN pi.quantity ELSE 0 END) as last_30d_incidents_quantity,

                    sum(case when incident_type !='EXTRA' and pi.stage = 'INVENTORY' and pi.location_id is not null then pi.quantity else 0 end) as incidents_quantity_location,
                    sum(case when incident_type ='CLEANUP_ADJUSTMENTS' then pi.quantity else 0 end) as cleanup_adjustments_quantity,

                    sum(case when incident_type ='EXTRA' then pi.quantity else 0 end) as extra_quantity,
                    sum(case when pi.stage = 'INVENTORY' and incident_type ='EXTRA' then pi.quantity else 0 end) as inventory_extra_quantity,
                    sum(case when pi.stage = 'PACKING' and incident_type ='EXTRA' then pi.quantity else 0 end) as packing_extra_quantity,

                    sum(case when incident_type ='DAMAGED' then pi.quantity else 0 end) as toat_damaged_quantity,

                    sum(case when after_sold is false and pi.stage = 'INVENTORY' and incident_type ='DAMAGED' then pi.quantity else 0 end) as inventory_damaged_quantity,
                        SUM(CASE WHEN DATE_DIFF(CURRENT_DATE(), date(pi.incident_at), DAY) <= 30 AND after_sold is false and pi.stage = 'INVENTORY' and incident_type ='DAMAGED' then pi.quantity ELSE 0 END) as last_30d_inventory_damaged_quantity

                    from {{ ref('stg_product_incidents')}}  as pi 
                    left join {{ ref('stg_line_items')}}  as li on  pi.line_item_id = li.line_item_id
                    left join {{ ref('stg_products')}}  as p on  p.line_item_id = li.line_item_id and p.product_id is not null
                where  pi.deleted_at is null
                group by p.product_id
            ),

            line_items_sold as (
                select
                    p.product_id,
                    count(li.line_item_id) as item_sold,
                    sum(li.quantity) as sold_quantity, 
                    sum(li.missing_quantity + li.damaged_quantity) as child_incident_quantity,

                    SUM(CASE WHEN DATE_DIFF(CURRENT_DATE(), case when li.delivery_date is null and li.order_type in ('IMPORT_INVENTORY', 'EXTRA','MOVEMENT') then date(li.created_at) else li.delivery_date end, DAY) <= 30 THEN li.quantity ELSE 0 END) as last_30d_sold_quantity
                    --SUM(CASE WHEN DATE_DIFF(CURRENT_DATE(), li.order_date, DAY) <= 30 THEN li.quantity ELSE 0 END) as last_30_days_quantity
                    from {{ ref('stg_line_items')}} as li
                    left join {{ ref('stg_products')}} as p on p.line_item_id = li.parent_line_item_id
                    --where p.product_id=149074
                group by 1
                
            ),
            ordered_quantity as (
                select
                    p.product_id,
                    SUM(CASE WHEN DATE_DIFF(CURRENT_DATE(), case when li.delivery_date is null and li.order_type in ('IMPORT_INVENTORY', 'EXTRA','MOVEMENT') then date(li.order_date) else li.delivery_date end, DAY) <= 30 THEN li.ordered_quantity ELSE 0 END) as last_30d_ordered_quantity
                    from {{ ref('stg_products')}} as p
                    left join {{ ref('fct_order_items')}} as li on p.line_item_id = li.line_item_id
                    group by 1

            )




        select
 
        --products
            p.* EXCEPT(quantity,published_quantity,remaining_quantity,visible,departure_date,created_at),
            p.quantity as inventory_product_quantity, --we need to take the order quanty form the line item not form the product, and  fulfilled_quantity from product (Awis)
            p.published_quantity,
            p.remaining_quantity,
            --p.departure_date,
            case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null  then date(p.created_at) else p.departure_date end as departure_date,
            
            p.created_at,

            s.supplier_name as Supplier,
            s.supplier_region as Origin,
            fs.feed_source_name as feed_source_name,
            origin_fs.feed_source_name as origin_feed_name,
            publishing_fs.feed_source_name as publishing_feed_name,
            out_fs.feed_source_name as out_feed_source_name,

            st.stock_name as Stock,
            --st.stock_model,

           -- st.stock_model_details,

            case 
                when st.stock_model_details in ('Reselling') then case when s.supplier_name = 'ASTRA Farms' then 'Commission Based - Astra Express' else 'Reselling' end
                when st.stock_model_details in ('Reselling Event') then case when s.supplier_name = 'ASTRA Farms' then 'Commission Based - Astra Express' else 'Reselling Event'  end
                else st.stock_model_details end as stock_model_details,

            case 
                when st.stock_model in ('Reselling') then case when s.supplier_name = 'ASTRA Farms' then 'Commission Based' else 'Reselling'
                end else st.stock_model end as stock_model,


            reseller.name as Reseller,
            concat(st.stock_id, " - ", st.stock_name, " - ", reseller.name  ) as full_stock_name,

            case when p.visible is true then 'Visible' else 'Not Visible' end as Visibility,
            case when p.remaining_quantity > 0 then 'Live Stock'  else 'Total Stock' end as live_stock,




        --product_locations
            pl.quantity as location_quantity,
            pl.remaining_quantity as location_remaining_quantity,
            pl.product_location_id,
            pl.location_id,
            pl.locationable_id,
            pl.created_at as fulfilled_at, --the time when the product added to loc in stock.
            --pl.updated_at,
            pl.empty_at, --damaged at.
            pl.locationable_type,
            pl.inventory_cycle_check_status,
            pl.labeled,
            pl.section_cycle_check,

            case 
when pl.quantity is null then 'not_scaned' 
when pl.quantity = p.quantity then 'scaned_good'
else 'scaned_flag' end as flag_1,


        --line_items
            li.order_date,
            li.ordered_quantity,
            ordered_quantity.last_30d_ordered_quantity,

            li.received_quantity,

            
            --li.inventory_quantity,
            li.fulfilled_quantity,
            li.record_type,
            li.record_type_details,
            li.order_status,
            li.fulfillment_status,
            li.warehouse,
            li.loc_status,
            li.fulfillment_mode,
            li.fulfillment,
            --li.delivery_date,
            case when li.delivery_date is null and li.order_type in ('IMPORT_INVENTORY', 'EXTRA','MOVEMENT') then date(li.order_date) else li.delivery_date end as delivery_date,
            li.User,
            li.order_type,
            li.Shipment,
            li.shipments_status,
            li.master_shipments_status,
            li.master_shipment,
            li.shipment_link,
            
            
            case 
            when date_diff(date(case when li.delivery_date is null and li.order_type in ('IMPORT_INVENTORY', 'EXTRA','MOVEMENT') then date(li.order_date) else li.delivery_date end)  ,current_date(), month) > 1 then 'Wrong date' 
            when case when li.delivery_date is null and li.order_type in ('IMPORT_INVENTORY', 'EXTRA','MOVEMENT') then date(li.order_date) else li.delivery_date end > current_date() then "Future" 
            when case when li.delivery_date is null and li.order_type in ('IMPORT_INVENTORY', 'EXTRA','MOVEMENT') then date(li.order_date) else li.delivery_date end = current_date() then "Today" 
            when case when li.delivery_date is null and li.order_type in ('IMPORT_INVENTORY', 'EXTRA','MOVEMENT') then date(li.order_date) else li.delivery_date end < current_date() then "Past" 
            else "cheak" end as select_delivery_date,

          
            case 
            when date_diff(date(case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null  then date(p.created_at) else p.departure_date end)  ,current_date(), month) > 1 then 'Wrong date' 
            when case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null  then date(p.created_at) else p.departure_date end > current_date() then "Future" 
            when case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null  then date(p.created_at) else p.departure_date end = current_date() then "Today" 
            when case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null  then date(p.created_at) else p.departure_date end < current_date() then "Past" 
            else "cheak" end as select_departure_date,



              
            

            

        --feed_source
            


        


        --line_items_sold
            lis.item_sold,
            lis.sold_quantity,
            lis.child_incident_quantity,
            lis.last_30d_sold_quantity,


        --product_incidents
            pi.incidents_count,
            pi.incidents_quantity,
            pi.last_30d_incidents_quantity,
            pi.last_30d_inventory_damaged_quantity,

            pi.incidents_quantity_location,
            pi.toat_damaged_quantity,
            pi.inventory_damaged_quantity,
            pi.extra_quantity,
            pi.inventory_extra_quantity,
            pi.packing_extra_quantity,
            pi.cleanup_adjustments_quantity,

        case when li.fulfillment_status = '2. Fulfilled - with Full Item Incident' and  incidents_quantity != p.quantity then 'red_flag' else null end as full_incident_check,

        case when COUNT(*) over (partition by p.product_id)>1 then 'multi-location' else null end as multi_location,
        row_number() over (partition by p.product_id) as row_number,
            
case 
    when p.product_name like '%Cutter%' THEN 'Accessories'
    when p.product_name like '%Arrangement%' THEN 'Accessories'
    when p.product_name like '%Artificial%' THEN 'Accessories'
    when p.product_name like '%Balloons%' THEN 'Accessories'
    when p.product_name like '%Baloon%' THEN 'Accessories'
    when p.product_name like '%Betula%' THEN 'Accessories'
    when p.product_name like '%Binding%' THEN 'Accessories'
    when p.product_name like '%Bouquet%' THEN 'Accessories'
    when p.product_name like '%Branches%' THEN 'Accessories'
    when p.product_name like '%Card%' THEN 'Accessories'
    when p.product_name like '%Cellophane%' THEN 'Accessories'
    when p.product_name like '%Ceramic%' THEN 'Accessories'
    when p.product_name like '%Christmass%' THEN 'Accessories'
    when p.product_name like '%Oasis%' THEN 'Accessories'
    when p.product_name like '%Clip%' THEN 'Accessories'
    when p.product_name like '%Color%' THEN 'Accessories'
    when p.product_name like '%Conical%' THEN 'Accessories'
    when p.product_name like '%Cortaderia%' THEN 'Accessories'
    when p.product_name like '%Crystal%' THEN 'Accessories'
    when p.product_name like '%Cutflower%' THEN 'Accessories'
    when p.product_name like '%Duct%' THEN 'Accessories'
    when p.product_name like '%Easter%' THEN 'Accessories'
    when p.product_name like '%Faza%' THEN 'Accessories'
    when p.product_name like '%Film%' THEN 'Accessories'
    when p.product_name like '%Floral%' THEN 'Accessories'
    when p.product_name like '%Flower%' THEN 'Accessories'
    when p.product_name like '%Foam%' THEN 'Accessories'
    when p.product_name like '%Follie%' THEN 'Accessories'
    when p.product_name like '%Glue%' THEN 'Accessories'
    when p.product_name like '%Golden%' THEN 'Accessories'
    when p.product_name like '%Green%' THEN 'Accessories'
    when p.product_name like '%Ideal%' THEN 'Accessories'
    when p.product_name like '%Kash%' THEN 'Accessories'
    when p.product_name like '%Khesh%' THEN 'Accessories'
    when p.product_name like '%Leaf%' THEN 'Accessories'
    when p.product_name like '%Metal%' THEN 'Accessories'
    when p.product_name like '%Natural%' THEN 'Accessories'
    when p.product_name like '%Oasis%' THEN 'Accessories'
    when p.product_name like '%Pampas%' THEN 'Accessories'
    when p.product_name like '%Pandanus%' THEN 'Accessories'
    when p.product_name like '%Paper%' THEN 'Accessories'
    when p.product_name like '%Plastic%' THEN 'Accessories'
    when p.product_name like '%Plate%' THEN 'Accessories'
    when p.product_name like '%Pr.%' THEN 'Accessories'
    when p.product_name like '%Preserved/dry%' THEN 'Accessories'
    when p.product_name like '%Preserverd%' THEN 'Accessories'
    when p.product_name like '%Raffia%' THEN 'Accessories'
    when p.product_name like '%Ribbon%' THEN 'Accessories'
    when p.product_name like '%Rondella%' THEN 'Accessories'
    when p.product_name like '%Rope%' THEN 'Accessories'
    when p.product_name like '%Satin%' THEN 'Accessories'
    when p.product_name like '%Saucer%' THEN 'Accessories'
    when p.product_name like '%Shining%' THEN 'Accessories'
    when p.product_name like '%Single%' THEN 'Accessories'
    when p.product_name like '%Steel%' THEN 'Accessories'
    when p.product_name like '%Sulfan%' THEN 'Accessories'
    when p.product_name like '%Tape%' THEN 'Accessories'
    when p.product_name like '%Vase%' THEN 'Accessories'
    when p.product_name like '%White%' THEN 'Accessories'
    when p.product_name like '%Wire%' THEN 'Accessories'
    when p.product_name like '%Wood%' THEN 'Accessories'
    when p.product_name like '%Wooden%' THEN 'Accessories'
    when p.product_name like '%Wrapping%' THEN 'Accessories'
    
else p.product_category end as new_category,

li.route_name,





        from {{ ref('stg_products')}} as p
        left join {{ ref('base_stocks')}} as st on p.stock_id = st.stock_id and p.reseller_id = st.reseller_id
        left join {{ ref('fct_order_items')}} as li on p.line_item_id = li.line_item_id
        left join {{ ref('base_suppliers')}} as s on s.supplier_id = li.supplier_id --was p.supplier_id
        left join {{ ref('stg_feed_sources')}} as origin_fs on p.origin_feed_source_id = origin_fs.feed_source_id 
        left join {{ ref('stg_feed_sources')}} as publishing_fs on p.publishing_feed_source_id = publishing_fs.feed_source_id 
        left join {{ ref('stg_feed_sources')}} as fs on p.feed_source_id = fs.feed_source_id 
        left join {{ ref('stg_feed_sources')}} as out_fs on st.out_feed_source_id = out_fs.feed_source_id 
        left join {{ ref('base_users')}} as reseller on reseller.id = p.reseller_id
        left join {{ ref('stg_product_locations')}} as pl on pl.locationable_id = p.product_id and pl.locationable_type = "Product"
        left join line_items_sold as lis on lis.product_id = p.product_id
        left join product_incidents as pi on pi.product_id = p.product_id
        left join ordered_quantity as ordered_quantity on ordered_quantity.product_id = p.product_id
        

        

    )
select * from CTE where row_number=1





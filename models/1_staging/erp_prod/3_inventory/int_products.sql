
with 
  product_incidents as (
                select 
                p.product_id, 
                    count(*) as incidents_count,
                    --sum(pi.quantity) as incidents_quantity,
                    sum(case when incident_type ='EXTRA' then 0 

                            when pi.stage = 'INVENTORY'   then 0

                            else  pi.quantity  end) as incidents_quantity,

                     sum(case when  pi.stage = 'INVENTORY' and incident_type ='DAMAGED' then pi.quantity else 0 end) as incident_quantity_inventory_dmaged,
                           


                    SUM(CASE WHEN DATE_DIFF(CURRENT_DATE(), date(pi.incident_at), DAY) <= 30 AND incident_type != 'EXTRA'  THEN pi.quantity ELSE 0 END) as last_30d_incidents_quantity,

                    sum(case when incident_type !='EXTRA' and pi.stage = 'INVENTORY' and pi.location_id is not null then pi.quantity else 0 end) as incidents_quantity_location,
                    sum(case when incident_type ='CLEANUP_ADJUSTMENTS' then pi.quantity else 0 end) as cleanup_adjustments_quantity,

                    sum(case when incident_type ='EXTRA' then pi.quantity else 0 end) as extra_quantity,
                    sum(case when pi.stage = 'INVENTORY' and incident_type ='EXTRA' then pi.quantity else 0 end) as inventory_extra_quantity,
                    sum(case when pi.stage = 'PACKING' and incident_type ='EXTRA' then pi.quantity else 0 end) as packing_extra_quantity,

                    sum(case when incident_type ='DAMAGED' then pi.quantity else 0 end) as toat_damaged_quantity,

                        SUM(CASE WHEN DATE_DIFF(CURRENT_DATE(), date(pi.incident_at), DAY) <= 30 AND  pi.stage = 'INVENTORY' and incident_type ='DAMAGED' then pi.quantity ELSE 0 END) as last_30d_incident_quantity_inventory_dmaged,
                        sum(case when incident_type !='EXTRA'  and pi.stage = 'RECEIVING' then  pi.quantity else 0 end) as incident_quantity_receiving_stage,
                        sum(case when incident_type !='EXTRA'  and pi.stage = 'PACKING' then  pi.quantity else 0 end) as incident_quantity_packing_stage,
                        sum(case when incident_type not in ('DAMAGED','EXTRA') and pi.stage = 'INVENTORY'  then pi.quantity else 0 end) as incident_quantity_inventory_stage,
                        sum(case when incident_type !='EXTRA' and pi.stage = 'DELIVERY'  then pi.quantity else 0 end) as incident_quantity_delivery_stage,
                        sum(case when incident_type !='EXTRA' and pi.stage = 'AFTER_RETURN'  then pi.quantity else 0 end) as incident_quantity_after_return_stage,



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
                    count(distinct li.customer_id) as customer_ordered,
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

            ),
 product_locations as (

                select
                      pl.locationable_id,
                      sum(pl.quantity) as location_quantity,
                      sum(pl.remaining_quantity) as remaining_quantity,
                      count(product_location_id) as location_count,

                      from {{ ref('stg_product_locations')}} AS pl    
                      LEFT JOIN {{ ref('stg_locations')}} AS loc ON pl.location_id = loc.location_id
                      LEFT JOIN {{ ref('stg_sections')}} AS sec ON sec.section_id = loc.section_id

                where pl.locationable_type = "Product" --and pl.locationable_id = 212559
                group by pl.locationable_id
            )

            
            
 select
 
  --products
            p.* EXCEPT(quantity,published_quantity,remaining_quantity,visible,product_expired_at,product_category,departure_date),
           -- case when pl.quantity is null and  lis.sold_quantity is not null   then li.ordered_quantity else pl.quantity end as location_quantity,      


--Date
    case when date(p.product_created_at) = date(li.order_date) then 'Match' else 'Check'  end as created_at_check,
    case when li.order_type = 'IMPORT_INVENTORY' and li.delivery_date is null then date(p.product_created_at) else li.delivery_date end as delivery_date,
    case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null then date(p.product_created_at) else p.departure_date end as departure_date,
        
    case 
        when date_diff(date(case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null then date(p.product_created_at) else p.departure_date end), current_date(), month) > 1 then 'Wrong date' 
        when case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null then date(p.product_created_at) else p.departure_date end > current_date() then 'Future' 
        when case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null then date(p.product_created_at) else p.departure_date end = current_date() then 'Today' 
        when case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null then date(p.product_created_at) else p.departure_date end < current_date() and 
            case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null then date(p.product_created_at) else p.departure_date end >= date_add(current_date(), INTERVAL -10 DAY) then 'last_10_days' 
        when case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null then date(p.product_created_at) else p.departure_date end < current_date() then 'Past' 
        else 'Check' 
    end as select_departure_date,



        case when pl.location_count is not null and  p.remaining_quantity > 0 and  st.stock_name = 'Inventory Stock' then p.remaining_quantity else 0 end as in_stock_quantity,


            p.quantity as inventory_product_quantity, --we need to take the order quanty form the line item not form the product, and  fulfilled_quantity from product (Awis)
            p.published_quantity,
            p.remaining_quantity,
            
            
            

            li.order_source,
            li.persona,

            

            
            os.supplier_name as Supplier, --orginal supplier form product table.
            s.supplier_region as Origin,

           --os.supplier_name as orginal_supplier,
           --s.supplier_name as Supplier, --supplier name form line_items table, the grandious not mapied for some reasion

            fs.feed_source_name as feed_source_name,
            origin_fs.feed_source_name as origin_feed_name,
            publishing_fs.feed_source_name as publishing_feed_name,
            out_fs.feed_source_name as out_feed_source_name,

            st.stock_name as Stock,
            
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
           pl.location_quantity,
           pl.remaining_quantity as location_remaining_quantity,
           pl.location_count,
           -- pl.product_location_id,
           -- pl.location_id,
           -- pl.locationable_id,
           -- pl.created_at as fulfilled_at, --the time when the product added to loc in stock.
            --pl.updated_at,
           -- pl.empty_at, --damaged at.
           -- pl.locationable_type,
           -- pl.inventory_cycle_check_status,
          --  pl.labeled,
           -- pl.section_cycle_check,

            case 
              when pl.location_count is null then 'not_scaned' 
              when pl.location_quantity = p.quantity then 'scaned_good'
            else 'scaned_flag' end as flag_1,
            case 
                when pl.location_count is null then 'not_scaned' 
                when pl.location_count =1 then 'scaned one location'
                when pl.location_count > 1 then 'scaned multi location'
                else 'check the logic'
                end as flag,


        --line_items
            li.order_date,
            li.ordered_quantity,
            
            ordered_quantity.last_30d_ordered_quantity,

            li.received_quantity,
            li.source_line_item_id,

            
            --li.inventory_quantity,
            li.fulfilled_quantity,
            li.li_record_type,
            li.li_record_type_details,
            li.order_status,
            li.fulfillment_status,
            li.fulfillment_status_details,
            li.warehouse, --from customer
            --w.warehouse_name as warehouse, --from stock

            case 
            when li.shipments_status not in ('PACKED','WAREHOUSED') then 0
            else li.ordered_quantity - COALESCE(pi.incident_quantity_packing_stage,0)
            end as  packed_quantity,


            case when li.requested_quantity is not null then li.requested_quantity else li.ordered_quantity end as requested_quantity,


            li.loc_status,
            li.fulfillment_mode,
            li.fulfillment,
            --li.delivery_date,
            li.User,
            li.order_type,
            li.Shipment,
            li.shipments_status,
            li.master_shipments_status,
            li.master_shipment,
            li.shipment_link,
            li.line_item_link,
            li.master_shipment_id,
            li.shipment_id,
            
            



              
            

        --feed_source
            


        


        --line_items_sold
            lis.item_sold,
            lis.sold_quantity,
            lis.child_incident_quantity,
            lis.last_30d_sold_quantity,
            lis.customer_ordered,


        --product_incidents
            pi.incidents_count,
            pi.incidents_quantity,
            pi.last_30d_incidents_quantity,
            pi.last_30d_incident_quantity_inventory_dmaged,

            pi.incidents_quantity_location,
            pi.toat_damaged_quantity,
            pi.incident_quantity_inventory_dmaged,
            pi.incident_quantity_inventory_stage,
            pi.extra_quantity,
            pi.inventory_extra_quantity,
            pi.packing_extra_quantity,
            pi.cleanup_adjustments_quantity,

            pi.incident_quantity_packing_stage,
            pi.incident_quantity_receiving_stage,

        case when li.fulfillment_status = '2. Fulfilled - with Full Item Incident' and  incidents_quantity != p.quantity then 'red_flag' else null end as full_incident_check,

            
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
    --when p.product_name like '%Crystal%' THEN 'Accessories'
    when p.product_name like '%Cutflower%' THEN 'Accessories'
    when p.product_name like '%Duct%' THEN 'Accessories'
    when p.product_name like '%Easter%' THEN 'Accessories'
    when p.product_name like '%Faza%' THEN 'Accessories'
    when p.product_name like '%Film%' THEN 'Accessories'
    when p.product_name like '%Floral%' THEN 'Accessories'
    --when p.product_name like '%Flower%' THEN 'Accessories'
    when p.product_name like '%Foam%' THEN 'Accessories'
    when p.product_name like '%Follie%' THEN 'Accessories'
    when p.product_name like '%Glue%' THEN 'Accessories'
    --when p.product_name like '%Golden%' THEN 'Accessories'
    --when p.product_name like '%Green%' THEN 'Accessories'
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
   -- when p.product_name like '%Single%' THEN 'Accessories'
    when p.product_name like '%Steel%' THEN 'Accessories'
    when p.product_name like '%Sulfan%' THEN 'Accessories'
    when p.product_name like '%Tape%' THEN 'Accessories'
    when p.product_name like '%Vase%' THEN 'Accessories'
   -- when p.product_name like '%White%' THEN 'Accessories'
    when p.product_name like '%Wire%' THEN 'Accessories'
    when p.product_name like '%Wood%' THEN 'Accessories'
    when p.product_name like '%Wooden%' THEN 'Accessories'
    when p.product_name like '%Wrapping%' THEN 'Accessories'
    
else INITCAP(p.product_category )
end as product_category,


li.route_name,



CASE
    WHEN p.product_subcategory IN (
      'rose', 'chrysanthemum', 'alstroemeria', 'lily', 'eustoma', 'gerbera',
      'garden-type-rose', 'delphinium', 'cymbidium', 'trachelium', 'sunflower',
      'anthurium', 'calla-lily', 'eryngium', 'veronica', 'aster', 'dendrobium',
      'chamelaucium', 'solidago', 'matthiola', 'brassica', 'dianthus-barbatus',
      'lilium', 'astilbe', 'tinted-roses', 'ornithogalum', 'vanda', 'asparagus',
      'aralia', 'bouvardia', 'waxed', 'ranunculus', 'monstera', 'pistacia',
      'aspidistra', 'mokkara', 'oxypetalum', 'pittosporum', 'ornamental-fruit',
      'banksia', 'symphoricarpos', 'freesia', 'hyacinthus', 'leucospermum',
      'anigozanthos', 'scabiosa', 'bupleurum', 'berzelia', 'anemone', 'lepidium',
      'skimmia', 'antirrhinum', 'ozothamnus', 'ammi', 'panicum', 'mentha',
      'oncidium', 'orchids', 'gladiolus', 'carthamus', 'acacia', 'astrantia',
      'tanacetum', 'paeonia', 'cordyline', 'amaranthus', 'fatsia', 'iris',
      'celosia', 'pennisetum'
    ) THEN DATE_ADD(li.departure_date, INTERVAL 7 DAY)

    WHEN p.product_subcategory IN (
      'spray-rose', 'carnation', 'tulip', 'eucalyptus', 'gypsophila', 'limonium',
      'dracaena', 'statice', 'craspedia', 'phalaenopsis', 'leucadendron',
      'protea', 'kaaps', 'brunia', 'green-plants', 'echeveria', 'crassula'
    ) THEN DATE_ADD(li.departure_date, INTERVAL 10 DAY)

    WHEN p.product_subcategory IN (
      'hydrangea', 'hypericum'
    ) THEN DATE_ADD(li.departure_date, INTERVAL 14 DAY)

    ELSE  DATE_ADD(li.departure_date, INTERVAL 7 DAY)
  END AS modified_expired_at,



CASE
    WHEN p.product_subcategory IN (
      'rose', 'chrysanthemum', 'alstroemeria', 'lily', 'eustoma', 'gerbera',
      'garden-type-rose', 'delphinium', 'cymbidium', 'trachelium', 'sunflower',
      'anthurium', 'calla-lily', 'eryngium', 'veronica', 'aster', 'dendrobium',
      'chamelaucium', 'solidago', 'matthiola', 'brassica', 'dianthus-barbatus',
      'lilium', 'astilbe', 'tinted-roses', 'ornithogalum', 'vanda', 'asparagus',
      'aralia', 'bouvardia', 'waxed', 'ranunculus', 'monstera', 'pistacia',
      'aspidistra', 'mokkara', 'oxypetalum', 'pittosporum', 'ornamental-fruit',
      'banksia', 'symphoricarpos', 'freesia', 'hyacinthus', 'leucospermum',
      'anigozanthos', 'scabiosa', 'bupleurum', 'berzelia', 'anemone', 'lepidium',
      'skimmia', 'antirrhinum', 'ozothamnus', 'ammi', 'panicum', 'mentha',
      'oncidium', 'orchids', 'gladiolus', 'carthamus', 'acacia', 'astrantia',
      'tanacetum', 'paeonia', 'cordyline', 'amaranthus', 'fatsia', 'iris',
      'celosia', 'pennisetum'
    ) THEN 7

    WHEN p.product_subcategory IN (
      'spray-rose', 'carnation', 'tulip', 'eucalyptus', 'gypsophila', 'limonium',
      'dracaena', 'statice', 'craspedia', 'phalaenopsis', 'leucadendron',
      'protea', 'kaaps', 'brunia', 'green-plants', 'echeveria', 'crassula'
    ) THEN 10

    WHEN p.product_subcategory IN (
      'hydrangea', 'hypericum'
    ) THEN 14

    ELSE 7
  END AS shelf_life_days,




--loc.label as location_name,
--sec.section_name as section,


case when p.product_expired_at is null then p.product_created_at else p.product_expired_at end as product_expired_at,
--concat(loc.label, " - ", sec.section_name) as Location,


case when ad.additional_items_report_id is not null then 'Additional Items' else null end as additional_items_check,

case 
    when ad.additional_items_report_id is not null then 'additional_inventory_item_id'
    when li.order_type ='RETURN' then 'return_inventory_item_id'
    when li.order_type ='MOVEMENT' then 'movement_inventory_item_id'
    else  'inventory_item_id'
end as inventory_item_type,


li.parent_parent_id_check,
li.parent_id_check,


        from {{ ref('stg_products')}} as p
        left join {{ ref('base_stocks')}} as st on p.stock_id = st.stock_id and p.reseller_id = st.reseller_id
        left join {{ ref('fct_order_items')}} as li on p.line_item_id = li.line_item_id
        left join {{ ref('base_suppliers')}} as s on s.supplier_id = li.supplier_id --was p.supplier_id
        left join {{ ref('base_suppliers')}} as os on os.supplier_id = p.original_supplier_id

        
        left join {{ ref('stg_feed_sources')}} as origin_fs on p.origin_feed_source_id = origin_fs.feed_source_id 
        left join {{ ref('stg_feed_sources')}} as publishing_fs on p.publishing_feed_source_id = publishing_fs.feed_source_id 
        left join {{ ref('stg_feed_sources')}} as fs on p.feed_source_id = fs.feed_source_id 
        left join {{ ref('stg_feed_sources')}} as out_fs on st.out_feed_source_id = out_fs.feed_source_id 
        left join {{ ref('base_users')}} as reseller on reseller.id = p.reseller_id
        left join product_locations as pl on pl.locationable_id = p.product_id 

        --left join {{ ref('stg_product_locations')}} as pl on pl.locationable_id = p.product_id and pl.locationable_type = "Product"
       -- left join {{ ref('stg_locations')}} as loc on pl.location_id=loc.location_id
       -- left join {{ ref('stg_sections')}} as sec on sec.section_id = loc.section_id

        left join {{ref('stg_additional_items_reports')}}  as ad on ad.line_item_id=p.line_item_id



        

        left join line_items_sold as lis on lis.product_id = p.product_id
        left join product_incidents as pi on pi.product_id = p.product_id
        left join ordered_quantity as ordered_quantity on ordered_quantity.product_id = p.product_id

        left join {{ref('base_warehouses')}} as w on w.warehouse_id = st.warehouse_id
      --  left join {{ref('base_warehouses')}} as w on w.warehouse_id = customer.warehouse_id

        

        

    





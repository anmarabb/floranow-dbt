
With source as 

    (
        select 
        
        id as line_item_id,
        
        case 
            when li.source_line_item_id is null and li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is not null then 'Reselling Purchase Orders'
            when li.source_line_item_id is null and li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is null and li.pricing_type in ('FOB','CIF') then 'Bulk Orders'
            when li.source_line_item_id is null and li.parent_line_item_id is null and li.ordering_stock_type is null and li.reseller_id is null then 'Customer Direct Sales Orders' --customer_direct_orders
            when li.source_line_item_id is null and li.ordering_stock_type is not null and li.reseller_id is null then 'Inventory Sales Orders' --customer_inventory_orders
            when li.source_line_item_id is null and li.ordering_stock_type is not null and li.reseller_id is not null then 'stock2stock'
            when li.source_line_item_id is not null and li.order_type = 'EXTRA' then 'EXTRA'
            when li.source_line_item_id is not null and li.order_type = 'RETURN' then 'RETURN' 
            when li.source_line_item_id is not null and li.order_type = 'MOVEMENT' then 'MOVEMENT'
            else 'cheack_my_logic'
            end as line_item_type,


            REGEXP_EXTRACT(permalink, r'/([^/]+)') AS product_crop , 
            REGEXP_EXTRACT(permalink, r'/(?:[^/]+)/([^/]+)') AS product_category,
            REGEXP_EXTRACT(permalink, r'/(?:[^/]+/){2}([^/]+)') AS product_subcategory,
           -- REGEXP_EXTRACT(permalink, r'/(?:[^/]+/){3}([^/]+)') AS column4,
           

           

        
        *
        
        
        from {{ source('erp_prod', 'line_items') }} as li
    )




select 

*,


--JASON extraction
--li.Properties, 
--li.categorization,
--li.tags,

current_timestamp() as ingestion_timestamp, 


from source as li


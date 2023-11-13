With source as (
 select * from {{ source('erp_prod', 'products') }} as p
 where p.deleted_at is  null
  and p.__hevo__marked_deleted is not true

)
select 

            --PK
                id as product_id,
            --FK
                line_item_id,
                stock_id,
                supplier_id,
                feed_source_id, --The feed source from which the product was ordered
                origin_feed_source_id, --The original feed source from which the product was ordered
                publishing_feed_source_id, --The feed source that will appear on the product when it is published on the marketplace
                reseller_id,
                order_id,
                supplier_product_id,
            

            --dim
                --date
                departure_date,
                expired_at as product_expired_at,
                created_at as product_created_at,
                updated_at,
                deleted_at,

                --product
                product_name,
                supplier_product_name,
                color,
                stem_length,
                stem_length_unit,
                images,
                properties,
                categorization,


                tags,
                number,
                sales_unit_name,

                visible,

                currency,
                fob_currency,
                landed_currency,





            --fct
                quantity,
                published_quantity,
                remaining_quantity,

                unit_fob_price,
                CAST(p.unit_landed_cost AS FLOAT64) as unit_landed_cost,
                unit_price,
                age,
                sales_unit,
                published_sales_unit,

                p.remaining_quantity * p.unit_price as remaining_value,
                p.remaining_quantity * CAST(p.unit_landed_cost AS FLOAT64) as landed_remaining_value,


concat( "https://erp.floranow.com/products/", p.id) as product_link,


        REGEXP_EXTRACT(permalink, r'/([^/]+)') as product_category, 
        REGEXP_EXTRACT(permalink, r'/(?:[^/]+)/([^/]+)') as product_subcategory,
        REGEXP_EXTRACT(permalink, r'/(?:[^/]+/){2}([^/]+)') as product_subcategory_2,


  --CONCAT('SKU_', LOWER(SUBSTR(MD5(product_name), 1, 8))) AS sku,
    CONCAT('SKU_', LOWER(TO_HEX(MD5(product_name)))) AS sku,


current_timestamp() as ingestion_timestamp,
 




from source as p

--where CONCAT('SKU_', LOWER(TO_HEX(MD5(product_name))))='SKU_c9ca13deb2644bebb2e567c45fc13b9c'
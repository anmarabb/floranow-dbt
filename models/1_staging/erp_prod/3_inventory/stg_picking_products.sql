With source as (
 select * from {{ source(var('erp_source'), 'picking_products') }}
)
select 
            --PK
                id as picking_product_id,
            --FK
            line_item_id,
            product_location_id,

            --dim
            status,

            created_at,
            updated_at,


            --fct
            quantity,


current_timestamp() as ingestion_timestamp,

from source as p
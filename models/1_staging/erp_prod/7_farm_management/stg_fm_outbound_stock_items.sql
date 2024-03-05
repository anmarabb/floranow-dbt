With source as (
 select * from {{ source(var('erp_source'), 'fm_outbound_stock_items') }}
)
select 
 
 
 --PK
   id as fm_outbound_stock_item_id,

 --FK
    fm_location_id,
    fm_product_id,
    fm_order_id,
    fm_box_item_id,


--dim
    status, 

    created_at,
    updated_at,
    production_date,

--fct

    quantity as fulfiled_quantity,


current_timestamp() as ingestion_timestamp,
 

from source as fmso


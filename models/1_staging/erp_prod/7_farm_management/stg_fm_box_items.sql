With source as (
 select * from {{ source(var('erp_source'), 'fm_box_items') }} as bi
)
select 
 
 
 --PK
   id as fm_box_item_id ,

 --FK
    fm_batch_item_id,
    fm_box_id,
    fm_product_id,
    user_id,
    fm_order_id,


--dim

    status,
    picking_status,


    created_at,
    updated_at,



--fct

    quantity as produced_quantity,
    quantity_unit,


current_timestamp() as ingestion_timestamp,
 




from source as bi




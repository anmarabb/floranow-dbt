With source as (
 select * from {{ source(var('erp_source'), 'fm_batch_items') }} as bhi
)
select 
 
 
 --PK
   id as fm_batch_item_id ,

 --FK
    fm_product_id,
    fm_batch_id,
    user_id,


--dim
    status,
    created_at,
    updated_at,

    date as production_date,


--fct

    quantity,
    packed_quantity,





current_timestamp() as ingestion_timestamp,
 




from source as bhi




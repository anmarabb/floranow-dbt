With source as (
 select * from {{ source(var('erp_source'), 'fm_inbound_stock_items') }}
)
select 
 
 
 --PK
   id as fm_inbound_stock_item_id ,

 --FK
    source_id,
    fm_warehouse_id,
    fm_product_id,


--dim
    source_type, --Fm::BoxItem,  Fm::AdditionalItem
    stock_type, --INBOUND
    status,  --STOCKED_IN, PENDING


    number,
    sequence,

    production_date,
    expiry_date_time,
    created_at,
    updated_at,

--fct
    quantity as inbound_quantity,

current_timestamp() as ingestion_timestamp,
 




from source as b


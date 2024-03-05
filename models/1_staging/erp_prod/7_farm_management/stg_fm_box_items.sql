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

    status, --INSPECTED, IN_TRANSIT (for shipmnet fm_order_id is null) , PENDING, null
    picking_status,  --NOT_PICKED, TOTALLY_PICKED, PARTIALLY_PICKED, null shipment


    created_at,
    updated_at,



--fct

    --quantity as produced_quantity,
    quantity_unit,
    case when fm_order_id is null then quantity else 0 end as produced_quantity,

    case when fm_order_id is not null and picking_status = 'TOTALLY_PICKED' then quantity else 0 end as packed_quantity,

    case when fm_order_id is not null and picking_status in ('PARTIALLY_PICKED','NOT_PICKED') then quantity else 0 end as unpacked_quantity,




current_timestamp() as ingestion_timestamp,
 




from source as bi




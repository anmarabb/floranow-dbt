With source as (
 select * from {{ source(var('erp_source'), 'fm_products') }}
)
select 
 
 
 --PK
   id as fm_product_id,

 --FK
    supplier_id,
    feed_source_id,
    fm_catalog_item_id,


--dim
    main_group,
    sub_group,
    properties,
    categorization,
    product_name,
    color,
    status,
    moq,
    images,

    failure_reason,

    created_at,
    updated_at,



--fct
    quantity,
    published_quantity,
    produced_quantity,
    available_quantity,


    fob_price,
    number,



    packing_rate,
    pn,


validated,



current_timestamp() as ingestion_timestamp,
 




from source as p

where p.__hevo__marked_deleted is false
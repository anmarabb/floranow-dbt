With source as (
 select * from {{ source(var('erp_source'), 'fm_boxes') }}
)
select 
 
 
 --PK
   id as fm_boxe_id,

 --FK
    user_id,
    fm_shipment_id,

--dim
    name,
    number,
    status,

    box_number,
    box_sequence,
    sequence,


    created_at,
    updated_at,


    
--fct



current_timestamp() as ingestion_timestamp,
 




from source as b


With source as (
 select * from {{ source(var('erp_source'), 'fm_shipments') }}
)
select 
 
 
 --PK
   id as fm_shipment_id,

 --FK
    user_id,
    fm_warehouse_id,
    destination_warehouse_id,



--dim

    name,
    number,
    status,
    shipment_type,

    departure_date,
    created_at,
    updated_at,
    departure_time,


--fct




current_timestamp() as ingestion_timestamp,
 




from source as sh


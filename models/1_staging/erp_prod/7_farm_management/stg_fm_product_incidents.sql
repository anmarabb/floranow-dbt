With source as (
 select * from {{ source(var('erp_source'), 'fm_product_incidents') }}
)
select 
 
 
 --PK
   id as fm_product_incident_id,

 --FK
    reported_by_id,
    --fm_stock_lot_count_id,
    fm_product_id,
    incidentable_id,

--dim

    incident_type, --SHORTAGE, MISSING, DAMAGED, OVER_BUDGET, DISPOSED
    incidentable_type,   --Fm::Order, Fm::BoxItem, Fm::Location

    stage,               --ORDER_FULFILLMENT, SHIPMENT_RECEIVING, INVENTORY
    status,              --REPORTED


    --deleted_at,
    created_at,
    updated_at,

    
--fct
    quantity,



current_timestamp() as ingestion_timestamp,
 




from source as pi
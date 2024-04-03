With source as (
 select * from {{ source(var('erp_source'), 'fm_orders') }}
)
select 
 
 
 --PK
   id as fm_order_id,

 --FK
    fm_product_id,
    --fm_box_item_id,
    --fm_shipment_id,
    destination_warehouse_id,
    --customer_id,

--dim

    customer_name,
    customer_email,
    customer_debtor_number,
    buyer_order_number, --line item

    number, --farm mangagmnet print stikers
    warehouse_name,
    fulfillment,
    status,



    created_at,
    updated_at,
    departure_date,
    delivery_date,

--fct

    quantity,


current_timestamp() as ingestion_timestamp,
 




from source as o


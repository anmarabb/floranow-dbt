With source as (
 select * from {{ source('erp_prod', 'additional_items_reports') }}
)
select 

 --PK
    id as additional_items_report_id,

 --FK
    line_item_id,
    feed_source_id,
    shipment_id,
    customer_id,
    warehouse_id,
    product_id,

    reported_by_id,
    rejected_by_id,
    approved_by_id,


--dim
        creation_stage, --INVENTORY, PACKING, RECEIVING
        status, -- APPROVED, FAILED, REJECTED, PROCESSING

        active,
        reject_reason,
        failure_reason,
        currency,

     --date
        created_at,
        failure_at,
        reported_at,
        approved_at,
        rejected_at,
        delivery_date,



--fct
    free_fob_price,
    selling_price,
    fob_price,
    quantity,



current_timestamp() as ingestion_timestamp,
 




from source as ad
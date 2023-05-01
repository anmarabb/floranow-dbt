With source as (
 select * from {{ source('erp_prod', 'shipments') }}
)
select 

            --PK
                id as shipment_id,
            --FK
                supplier_id,
                warehouse_id,
                customer_id,
                master_shipment_id,



            --dim
                name as Shipment,
                status, --DRAFT, PACKED, WAREHOUSED, MISSING, INSPECTED, CANCELED
                fulfillment, --UNACCOUNTED, PARTIAL, SUCCEED, FAILED
                receiving_way,  --BY_BOX, BY_CLIENT, null


                packing_type, --null
                customer_type, --null
                
                cancellation_reason, --null
                number,
                note, --null
                previous_masters,
                is_local,



                invoice_uploaded_by,
                proforma_uploaded_by,
                canceled_by_id,



            --date
                received_at,
                created_at,
                updated_at,
                departure_date,
                canceled_at,
                deleted_at,

            --fct
                total_quantity,
                total_received_quantity,
                total_missing_quantity,
                total_damaged_quantity,

                total_fob,
                total_received_fob,
                total_missing_fob,
                total_damaged_fob,
                
                invoice_amount,
                proforma_amount,

                shipping_boxes_count,
                warehousing_boxes_count,



                


            

    
    


current_timestamp() as ingestion_timestamp,
 




from source as sh
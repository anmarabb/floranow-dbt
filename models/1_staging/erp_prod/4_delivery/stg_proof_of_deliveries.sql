With source as (
 select * from {{ source(var('erp_source'), 'proof_of_deliveries') }}
)
select 

            --PK
                id as proof_of_delivery_id,
            --FK
            route_id,
            dispatched_by_id,
            invoices_ids,
            customer_id,
            skipped_by_id,
            split_by_id,
            split_from_id,
            moved_by_id,



            --dim

                --date
                dispatched_at,
                created_at,
                deleted_at,
                delivery_date,
                updated_at,
                ready_at,
                delivered_at,
                window_starts_at,
                skipped_at,
                split_at,
                moved_at,



                summary,
               -- creation_condition,
                source_type,
                number,
                barcode,
                status as pod_status, --DRAFT, READY, DISPATCHED, DELIVERED, SKIPPED
                discontinued,
                sequence_number,
                sequence,
                

            
            --fct
            quality_review_status,


/*
            invoices_ids,
            REGEXP_REPLACE(REPLACE(SPLIT(invoices_ids, ',')[SAFE_ORDINAL(1)], '{', ''), '}', '') AS value1,
            REGEXP_REPLACE(REPLACE(SPLIT(invoices_ids, ',')[SAFE_ORDINAL(2)], '{', ''), '}', '') AS value2,
            REGEXP_REPLACE(REPLACE(SPLIT(invoices_ids, ',')[SAFE_ORDINAL(3)], '{', ''), '}', '') AS value3,
            REGEXP_REPLACE(REPLACE(SPLIT(invoices_ids, ',')[SAFE_ORDINAL(4)], '{', ''), '}', '') AS value4,
            REGEXP_REPLACE(REPLACE(SPLIT(invoices_ids, ',')[SAFE_ORDINAL(5)], '{', ''), '}', '') AS value5,
*/

  CASE
    WHEN invoices_ids = '{}' THEN 0
    ELSE ARRAY_LENGTH(SPLIT(REPLACE(REPLACE(invoices_ids, '{', ''), '}', ''), ','))
  END AS ids_count,



delivery_at,




current_timestamp() as ingestion_timestamp,
 




from source as pod
--where ARRAY_LENGTH(SPLIT(REPLACE(REPLACE(invoices_ids, '{', ''), '}', ''), ','))>1

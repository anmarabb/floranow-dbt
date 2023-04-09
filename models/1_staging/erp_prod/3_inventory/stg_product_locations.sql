With source as (
 select * from {{ source('erp_prod', 'product_locations') }}
)
select 

            --PK
                id as product_location_id,
            --FK
            location_id,
            locationable_id,


            --dim
                --date
                created_at,
                updated_at,
                empty_at,



                locationable_type,
                inventory_cycle_check_status,
                labeled,
                section_cycle_check,



            --fct
            quantity,
            remaining_quantity,


current_timestamp() as ingestion_timestamp,
 




from source as p
With source as (
 select * from {{ source('erp_prod', 'move_items') }}
)
select 
            --PK
                id as move_item_id,
            --FK
                user_id,
                documentable_id,
            --dim
                source_system,
                currency,
                entry_type,
                reconciled,

            --date
                date,
                updated_at,



             --fct
                residual,
                prev_residual,
                balance,
           

current_timestamp() as ingestion_timestamp

 
from source
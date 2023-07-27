With source as (
 select * from {{ source('erp_prod', 'payments') }}
)
select 
            --PK
                id as payment_id,

            --FK
                invoice_id as invoice_header_id,
                payment_transaction_id,
                credit_note_id,
                debit_move_item_id,
                credit_move_item_id,
                external_source_id,
                user_id,
            
            --dim
                payment_type,
                currency,
                added_by,
                approved_by,
                odoo_imported,
                source_system,
                odoo_synced,

            --date
                created_at,
                updated_at,
                deleted_at,

            --fct
                credit_note_amount,
                paid_amount,
                total_amount,

                
current_timestamp() as ingestion_timestamp

 
from source as i 

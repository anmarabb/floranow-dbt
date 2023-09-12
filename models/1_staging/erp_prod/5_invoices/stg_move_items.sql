With source as (
 select * from {{ source('erp_prod', 'move_items') }}
)
select 
            --PK
                id as move_item_id,
            --FK
                user_id,
                documentable_id,
                company_id,
            --dim
                source_system, -- ODOO, FLORANOW_ERP, FLORISOFT
                currency,
                entry_type,  --CREDIT, DEBIT
                case when reconciled is true then 'reconciled' else null end as reconciled,
                documentable_type, --Invoice, PaymentTransaction, null

            --date
                date,
                updated_at,
                created_at,
                deleted_at,



             --fct
                residual,
                prev_residual,
                balance,
           

current_timestamp() as ingestion_timestamp

 
from source
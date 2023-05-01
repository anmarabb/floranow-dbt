With source as (
 select * from {{ source('erp_prod', 'payment_transactions') }}
)
select 
            --PK
                id as payment_transaction_id,
            --FK
                user_id,
                bank_account_id,
                financial_administration_id,
                odoo_id,

                payment_received_by,
                created_by,
                updated_by,
                collected_by,
                added_by,

            --dim
                payment_transaction_type,
                adjustment_status,
                payment_method,
                transaction_type,
                payment_gateway,
                status,
                approved,
                trx_reference,
                currency,


            --date
                payment_received_at,
                collected_at,
                created_at,
                updated_at,




            --fct
                remaining_amount,
                total_amount,
                credit_note_amount,
                paid_amount,


current_timestamp() as ingestion_timestamp

 
from source as i 
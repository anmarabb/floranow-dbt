With source as (
 select * from {{ source(var('erp_source'), 'payment_transactions') }}
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
                transaction_type,            --EXTERNAL, MANUAL, ONLINE, IN_SHOP
                payment_transaction_type,    --ADVANCED, NORMAL, null
                adjustment_status,           --NOT_ADJUSTED, TOTALLY_ADJUSTED, PARTIALLY_ADJUSTED
                payment_method,              --BANK_TRANSFER, VISA_CARD, PAYMENT_BY_CREDIT, CASH, CHEQUE, WRITE_OFF, CREDIT, OVER_PAYED, OFFSET, OTHERS
                payment_gateway,
                status,                      -- DRAFT, SUCCESS,  FAILED, PROCESSING, CANCELED
                approved,
                trx_reference,
                currency,
                number,
                approval_code,

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
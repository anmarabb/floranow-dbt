With source as (
 select * from {{ source('erp_prod', 'invoices') }}
)
select 
            --PK
                i.id as invoice_id,
            --FK
                parent_invoice_id,
                customer_id,
                proof_of_delivery_id,
                in_shop_order_number,
                purchase_order_number,



                case --financial ID
                    when i.financial_administration_id = 1 then 'KSA'
                    when i.financial_administration_id = 2 then 'UAE'
                    when i.financial_administration_id = 3 then 'Jordan'
                    when i.financial_administration_id = 4 then 'kuwait'
                    when i.financial_administration_id = 5 then 'Qatar'
                    when i.financial_administration_id = 6 then 'Bulk'
                    when i.financial_administration_id = 7 then 'Internal'
                    else 'check_my_logic'
                end as financial_administration,

                printed_by_id,
                deleted_by,
                canceled_by_id,
                paid_by,
                finalized_by,
                void_by,
                voided_by_id,
                created_by,

            --dim
                --date
                canceled_at,
                created_at as proforma_at,  --proforma_at,
                printed_at,  --invoiced_at,
                updated_at,
                signed_at,
                finalized_at,
                last_payment_at,
                due_date,
                voided_at,
                last_send_email_at,
                items_collection_date,
                paid_at,
                deleted_at,


                --dim
                creation_condition,
                language,
                number,
                currency,
                case when i.invoice_type = 1 then 'credit note' else 'invoice' end as invoice_type,
                items_collection_method,
                items_source_type,
                generation_type,
                

case 
    when i.invoice_type = 1 and i.generation_type ='AUTO' then 'Credit Note - AUTO'
    when i.invoice_type = 1 and i.generation_type ='MANUAL' then 'Credit Note - MANUAL'
    when i.invoice_type = 0 and i.generation_type ='MANUAL' then 'Invoice - MANUAL'
    when i.invoice_type = 0 and i.generation_type ='AUTO' then 'Invoice - AUTO'
    else null
end as record_type,


current_timestamp() as ingestion_timestamp

 
from source as i 


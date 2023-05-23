With source as (
 select * from {{ source('erp_prod', 'invoices') }}
)
select 
            --PK
                i.id as invoice_header_id,
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
                created_at as invoice_header_created_at,  --proforma_at,
                printed_at as invoice_header_printed_at,  --invoiced_at,
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
                case
                when i.status = 0 then "Draft"
                when i.status = 1 then "signed"
                when i.status = 2 then "Open"
                when i.status = 3 then "Printed"
                when i.status = 6 then "Closed"
                when i.status = 7 then "Canceled"
                when i.status = 8 then "Rejected"
                when i.status = 9 then "voided"
                when i.status is null then ""
                else "check_my_logic"
                end as invoice_header_status,


                --dim
                creation_condition,
                language,
                number,
                currency,
                case 
                when i.invoice_type = 1 then 'credit note' 
                when i.invoice_type = 0 then 'invoice'
                else 'check' end as invoice_header_type,
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


        --fct
            remaining_amount,
            tax_rate,
            prev_remaining_amount,
            total_tax,
            total_amount,
            paid_amount,
            discount_amount,
            price_without_discount,



current_timestamp() as ingestion_timestamp

 
from source as i 


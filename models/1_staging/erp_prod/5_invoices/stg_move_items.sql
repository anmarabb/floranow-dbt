With source as (
 select mi.*,  i.due_date, i.invoice_header_id,
 from {{ source(var('erp_source'), 'move_items') }} as mi
 left join {{ref('stg_invoices')}} as i on mi.documentable_id = i.invoice_header_id and mi.documentable_type = 'Invoice' and mi.entry_type = 'DEBIT'

)
select 
            --PK
                id as move_item_id,
            --FK
                user_id,
                documentable_id,
                external_source_id,
                company_id,
                financial_administration_id,
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
                creation_date,



             --fct
                residual,
                prev_residual,
                balance,
                case when entry_type = 'DEBIT' then balance else 0 end as total_debits, --The sum of all the invoices issued to the customer. total_debits = sum(total_amount) for printed invoice. + VAT
                case when entry_type = 'CREDIT' then balance else 0 end as total_credits, --The sum of all the payments received from the customer and all the credit notes issued to the customer. total_credits= payments + credit_nots + other_credit other_credit: from odoo


    --case when invoice_header_id is not null then date(due_date) else date(mi.date) end as due_date,
        case when due_date is not null then date(due_date) else date(mi.date) end as due_date,

    --due_date,


           

current_timestamp() as ingestion_timestamp

 
from source as mi
where 
mi.deleted_at is null
and mi.balance != 0
and mi.__hevo__marked_deleted is not true

--and mi.documentable_id is not null
--and ((mi.entry_type = 'DEBIT' AND round(residual, 2) >= 0) OR (mi.entry_type = 'CREDIT' AND round(mi.residual, 2) <= 0))

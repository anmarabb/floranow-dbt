with

source as ( 
        
select  

case when mi.entry_type = 'CREDIT' and mi.documentable_type = 'PaymentTransaction' then mi.balance else 0 end as payments,

i.total_tax as invoice_total_tax,
cn.total_tax as credit_note_total_tax,
 
COALESCE(i.total_tax,0) + COALESCE(cn.total_tax,0) as total_tax,


case when mi.entry_type = 'CREDIT' and mi.documentable_type = 'Invoice' then mi.balance else 0 end as credit_nots_with_tax,
case when mi.entry_type = 'CREDIT' and mi.documentable_type = 'Invoice' then (mi.balance - COALESCE(cn.total_tax,0)) else 0 end as credit_note,
case when mi.entry_type = 'CREDIT' and (mi.documentable_id is null or mi.documentable_type is null) then mi.balance end  as other_credit,

case when mi.entry_type = 'DEBIT' then mi.balance else 0 end as gross_revenue_with_tax,
case when mi.entry_type = 'DEBIT' then (mi.balance - COALESCE(i.total_tax,0)) else 0 end as gross_revenue,


--case when mi.entry_type = 'CREDIT' and mi.documentable_type = 'Invoice' then mi.residual else 0 end as unreconciled_credits_CN,
--case when mi.entry_type = 'CREDIT' and mi.documentable_type = 'PaymentTransaction' then mi.residual else 0 end as unreconciled_credits_PT,
--case when mi.entry_type = 'CREDIT' and mi.documentable_type = 'Invoice' then (round(mi.balance,2) - round(mi.residual,2)) else 0 end as reconciled_credits_CN,

--case when mi.entry_type = 'CREDIT' and mi.documentable_type = 'PaymentTransaction' then (round(mi.balance,2) - round(mi.residual,2)) else 0 end as reconciled_credits_PT,


--case when mi.entry_type = 'DEBIT'  then mi.residual else 0 end as unreconciled_debits_INV, -- unpaid invoice
--case when mi.entry_type = 'DEBIT'  then (round(mi.balance,2) - round(mi.residual,2)) else 0 end as reconciled_debits_INV,  --paid invoices

mi.* EXCEPT(created_at),



--LTV = total_debits-total_credits. (net revinew)
--total_debits,  --gross reve
 --total creidt not =  case when mi.entry_type = 'CREDIT' and mi.documentable_type = 'Invoice' then mi.balance else 0 end as total_credits_CN,
-- CN Rate (total_credits_CN / total_debits). 
--Collection Rate. (  unreconciled_debits / LTV)



case when mi.entry_type = 'DEBIT' then mi.residual else 0 end as unreconciled_debits, --outstanding sum(residual)



case when mi.entry_type = 'CREDIT' then mi.residual else 0 end as unreconciled_credits,




case when mi.entry_type='CREDIT' then abs (CNmi.residual) else 0 end as CN_amount,
case when mi.entry_type='CREDIT' then abs (PTmi.residual) else 0 end as PT_amount,



case when (pt.payment_transaction_id is null and cn.invoice_header_id is null and mi.source_system = 'ODOO')   then 'ODOO' else pt.number end as payment_transaction_number,
case when (pt.payment_transaction_id is null and cn.invoice_header_id is null and mi.source_system = 'ODOO')   then 'ODOO' else cn.invoice_number end as credit_note_number,

pt.approval_code,
pt.transaction_type,

case when pt.payment_gateway=0 then 'telr' else null end as payment_gateway,


--date
    case when mi.date is not null then mi.date else mi.created_at end as created_at, 
    case when pt.payment_received_at is not null then pt.payment_received_at else mi.created_at end as payment_received_at,
    
 

case when mi.documentable_id is not null and mi.documentable_type is not null then

(case when mi.documentable_type = 'PaymentTransaction' then pt.number else
(case when mi.entry_type = 'DEBIT' then i.invoice_number else cn.invoice_number end) 
 end )
 else null end as doc_number,









    CASE
        WHEN mi.documentable_id IS NOT NULL AND mi.documentable_type IS NOT NULL THEN
            CASE 
                WHEN mi.documentable_type = 'PaymentTransaction' THEN 'PT' 
                WHEN mi.entry_type = 'DEBIT' THEN 'INV' 
                ELSE 'CN' 
            END
    END AS doc_type,



customer.name as Customer,
customer.account_manager,
customer.debtor_number,
customer.city,
customer.user_category,
customer.Warehouse as warehouse,
customer.payment_term,
customer.credit_limit,
customer.customer_type,
customer.account_type,
customer.user_validity_filter,
--fct
   -- -mi.balance as paid_amount,
  --  -(mi.balance - mi.residual) as reconciled_amount,
   -- -mi.residual as un_reconciled_amount,




-----

case 
when mi.company_id = 3 then 'Bloomax Flowers LTD'
when mi.company_id = 2 then 'Global Floral Arabia tr'
when mi.company_id = 1 then 'Flora Express Flower Trading LLC'
else  'cheack'
end as company_name,



fn.name as financial_administration,

pt.payment_method,


case when date_diff( cast(current_date() as date ),cast(mi.date as date), DAY) <= 30 then mi.residual else 0 end as up_to_30_days,
case when date_diff( cast(current_date() as date ),cast(mi.date as date), DAY) > 30 and date_diff( cast(current_date() as date ),cast(mi.date as date), DAY) <= 60 then mi.residual else 0 end as between_31_to_60_days,
case when date_diff( cast(current_date() as date ),cast(mi.date as date), DAY) > 60 and date_diff( cast(current_date() as date ),cast(mi.date as date), DAY) <= 90 then mi.residual else 0 end as between_61_to_90_days,
case when date_diff( cast(current_date() as date ),cast(mi.date as date), DAY) > 90 and date_diff( cast(current_date() as date ),cast(mi.date as date), DAY) <= 120 then mi.residual else 0 end as between_91_to_120_days,
case when date_diff( cast(current_date() as date ),cast(mi.date as date), DAY) > 120 then mi.residual else 0 end as more_than_120_days,

case when date_diff(current_date(),date(mi.date), MONTH) = 1 then i.remaining_amount else 0 end as m_1_remaining,
case when date_diff(current_date(),date(mi.date), MONTH) = 2 then i.remaining_amount else 0 end as m_2_remaining,
case when date_diff(current_date(),date(mi.date), MONTH) = 3 then i.remaining_amount else 0 end as m_3_remaining,
case when date_diff(current_date(),date(mi.date), MONTH) = 0 then i.remaining_amount else 0 end as mtd_remaining,

case when mi.due_date < current_date() then mi.residual else 0 end as collectible_amount,

case when mi.due_date < current_date() then mi.residual else 0 end as past_due_receivable , --Overdue Receivable
case when mi.due_date >= current_date() then mi.residual else 0 end as current_due_receivable  , --current_receivables, unmatured_receivables

--amount of money that has not been paid by its due date

--"Current Due Receivable" amount is owed but not yet reached its due date for payment.

--Total Receivable = Current Due Receivable + Past-Due Receivable
case when date_diff(cast(current_date() as date), cast(mi.due_date as date), DAY) > 0 and date_diff(cast(current_date() as date), cast(mi.due_date as date), DAY) <= 30 then mi.residual else 0 end as up_to_30_days_past_due,
case when date_diff( cast(current_date() as date ),cast(mi.due_date as date), DAY) > 30 and date_diff( cast(current_date() as date ),cast(mi.due_date as date), DAY) <= 60 then mi.residual else 0 end as between_31_to_60_days_past_due,
case when date_diff( cast(current_date() as date ),cast(mi.due_date as date), DAY) > 60 and date_diff( cast(current_date() as date ),cast(mi.due_date as date), DAY) <= 90 then mi.residual else 0 end as between_61_to_90_days_past_due,
case when date_diff( cast(current_date() as date ),cast(mi.due_date as date), DAY) > 90 and date_diff( cast(current_date() as date ),cast(mi.due_date as date), DAY) <= 120 then mi.residual else 0 end as between_91_to_120_days_past_due,
case when date_diff( cast(current_date() as date ),cast(mi.due_date as date), DAY) > 120 then mi.residual else 0 end as more_than_120_days_past_due,

DATE_DIFF(DATE(mi.due_date) ,CURRENT_DATE() , DAY) as days_to_due_date,

   -- current_timestamp() as insertion_timestamp, 

from {{ ref('stg_move_items')}} as mi
left join {{ ref('base_users') }} as customer on customer.id = mi.user_id
left join {{ ref('stg_payment_transactions') }} as pt on pt.payment_transaction_id = mi.documentable_id and mi.documentable_type = 'PaymentTransaction' and  mi.entry_type = 'CREDIT'
left join {{ ref('stg_financial_administrations') }} as fn on fn.id = mi.financial_administration_id
left join {{ source(var('erp_source'), 'bank_accounts') }} as ba on pt.bank_account_id = ba.id

left join {{ref('stg_invoices')}} as i on mi.documentable_id = i.invoice_header_id and mi.documentable_type = 'Invoice' and mi.entry_type = 'DEBIT'
left join {{ref('stg_invoices')}} as cn on mi.documentable_id = cn.invoice_header_id and mi.documentable_type = 'Invoice' and mi.entry_type = 'CREDIT'


left join {{ ref('stg_move_items')}} as CNmi on  CNmi.move_item_id = mi.move_item_id and CNmi.documentable_type = 'Invoice' 
left join {{ ref('stg_move_items')}} as PTmi on  PTmi.move_item_id = mi.move_item_id and PTmi.documentable_type = 'PaymentTransaction' 

--where customer.deleted_at is null



    )

select * from source
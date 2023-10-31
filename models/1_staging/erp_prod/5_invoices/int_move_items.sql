with

source as ( 
        
select     

mi.* EXCEPT(created_at),

--date
    case when mi.date is not null then mi.date else mi.created_at end as created_at, 
    case when pt.payment_received_at is not null then pt.payment_received_at else mi.created_at end as received_at, 

case when mi.documentable_id is not null and mi.documentable_type is not null then

(case when mi.documentable_type = 'PaymentTransaction' then pt.number else
(case when mi.entry_type = 'DEBIT' then i.invoice_number else cn.invoice_number end) 
 end )
 else null end as doc_number,

case when entry_type = 'CREDIT' then balance else 0 end as total_credits,


case when entry_type = 'DEBIT' then balance else 0 end as total_debits,


case when entry_type = 'CREDIT' and mi.documentable_type = 'PaymentTransaction' then balance else 0 end as payments,
case when entry_type = 'CREDIT' and mi.documentable_type = 'Invoice' then balance else 0 end as credit_nots,


case when entry_type = 'CREDIT' then residual else 0 end as unreconciled_credits,
case when entry_type = 'DEBIT' then residual else 0 end as unreconciled_debits,




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
customer.company_name,
customer.city,
customer.user_category,
customer.Warehouse,

--fct
   -- -mi.balance as paid_amount,
  --  -(mi.balance - mi.residual) as reconciled_amount,
   -- -mi.residual as un_reconciled_amount,





-----
CASE
	WHEN customer.warehouse_id IN (10, 79, 76, 43) THEN
	CASE
		WHEN mi.date > '2023-07-09' THEN customer.company_id 
		WHEN mi.date <= '2023-07-09' THEN 
		CASE 
			WHEN REGEXP_CONTAINS(customer.debtor_number, r'^b') THEN 3
			WHEN NOT REGEXP_CONTAINS(customer.debtor_number, r'^b') THEN 
			CASE 
				WHEN mi.source_system = 'ODOO' THEN 3
				WHEN mi.source_system IN ('FLORANOW_ERP', 'FLORISOFT') THEN customer.company_id 
		    END
		END
	END
	WHEN customer.warehouse_id NOT IN (10, 79, 76, 43) OR customer.warehouse_id IS NULL THEN
	CASE
		WHEN mi.source_system = 'ODOO' THEN 3
		WHEN mi.source_system IN ('FLORANOW_ERP', 'FLORISOFT') THEN customer.company_id 
	END
END AS reporting_company_id,

fn.name as financial_administration,


   -- current_timestamp() as insertion_timestamp, 

from {{ ref('stg_move_items')}} as mi
left join {{ ref('base_users') }} as customer on customer.id = mi.user_id
left join {{ ref('stg_payment_transactions') }} as pt on pt.payment_transaction_id = mi.documentable_id and mi.documentable_type = 'PaymentTransaction' and  mi.entry_type = 'CREDIT'
left join {{ source('erp_prod', 'financial_administrations') }} as fn on fn.id = customer.financial_administration_id
left join {{ source('erp_prod', 'bank_accounts') }} as ba on pt.bank_account_id = ba.id

left join {{ref('stg_invoices')}} as i on mi.documentable_id = i.invoice_header_id and mi.documentable_type = 'Invoice' and mi.entry_type = 'DEBIT'
left join {{ref('stg_invoices')}} as cn on mi.documentable_id = cn.invoice_header_id and mi.documentable_type = 'Invoice' and mi.entry_type = 'CREDIT'

where customer.deleted_at is null
and  mi.deleted_at is null
--and customer.financial_administration_id = 1
and mi.balance != 0
and ((mi.entry_type = 'DEBIT' AND round(residual, 2) >= 0) OR (mi.entry_type = 'CREDIT' AND round(mi.residual, 2) <= 0))



    )

select * from source
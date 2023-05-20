SELECT
stg_users.financial_administration,
i.printed_at,
stg_users.debtor_number,
stg_users.customer,
stg_users.client_category,
stg_users.city,
stg_users.account_manager,
case when i.invoice_type = 0 then "invoice" when i.invoice_type = 1 then "credit note" else "Null" end as invoice_type,
i.number as invoice_number,
i.total_amount - i.total_tax as total_amount_without_tax,
i.total_tax,
i.total_amount, --Invoice Total (Subtotal Amount + VAT)
i.paid_amount,
i.remaining_amount, --i.total_amount - i.paid_amount as pending_amount,
i.currency,
case when i.source_type = 'EXTERNAL' then 'Florisoft' when i.source_type = 'INTERNAL' then 'ERP' else 'check_my_logic' end as source_type, i.generation_type,

from `floranow.erp_prod.invoices` as i
left join `floranow.Floranow_ERP.users` as stg_users on stg_users.id = i.customer_id
left join `floranow.Floranow_ERP.stg_paymnets` as stg_paymnets on stg_paymnets.invoice_id = i.id
left join `floranow.Floranow_ERP.stg_invoice_items` as stg_invoice_items on stg_invoice_items.invoice_id = i.id

where  i.status in (1,3) and  stg_users.financial_administration = 'KSA'  and  date_diff(cast(current_date() as date ),cast(i.printed_at as date), MONTH) = 1
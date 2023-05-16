with 
prep_registered_clients as (select financial_administration,count(*) as registered_clients from `floranow.Floranow_ERP.users` where account_type in ('External') group by financial_administration)   
SELECT

i.id,
i.number as invoice_number,
i.printed_at,
stg_users.debtor_number,
stg_users.financial_administration,
stg_users.customer,
stg_users.account_manager,
stg_users.city,
stg_users.client_category,
stg_users.country,
stg_users.customer_type,
stg_users.payment_term,


i.total_amount - i.total_tax as total_amount_without_tax,
i.total_tax,
i.total_amount, --Invoice Total (Subtotal Amount + VAT)
i.currency,
i.paid_amount,
i.remaining_amount, --i.total_amount - i.paid_amount as pending_amount,


case
when i.payment_status = 0 then "Not paid"
when i.payment_status = 1 then "Partially paid"
when i.payment_status = 2 then "Totally paid "
else "Null"
end as payment_status,



case
when i.status = 0 then "Draft"
when i.status = 1 then "signed"
when i.status = 2 then "Open"
when i.status = 3 then "Printed"
when i.status = 6 then "Closed"
when i.status = 7 then "Canceled"
when i.status = 8 then "Rejected"
else "Null"
end as status,



case
--when i.invoice_type = 0 and i.total_amount = 0 then "zero-invoice"
when i.invoice_type = 0 then "invoice"
when i.invoice_type = 1 then "credit note"
else "Null"
end as invoice_type,




case
when i.source_type = 'EXTERNAL' then 'Florisoft'
when i.source_type = 'INTERNAL' then 'ERP'
else 'check_my_logic'
end as source_type,

i.deleted_at,

from `floranow.erp_prod.invoices` as i
left join `floranow.Floranow_ERP.users` as stg_users on stg_users.id = i.customer_id
left join prep_registered_clients as prep_registered_clients on prep_registered_clients.financial_administration = stg_users.financial_administration
left join `floranow.Floranow_ERP.stg_paymnets` as stg_paymnets on stg_paymnets.invoice_id = i.id
left join `floranow.Floranow_ERP.stg_invoice_items` as stg_invoice_items on stg_invoice_items.invoice_id = i.id

where i.status in (1,3) and  date_diff(cast(current_date() as date ),cast(i.printed_at as date), YEAR) = 0
and stg_users.master_account = 'Alissar Flowers' order by i.printed_at


-- and stg_users.debtor_number = '123654'
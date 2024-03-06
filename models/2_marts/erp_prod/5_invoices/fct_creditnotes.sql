with

source as ( 

select

invoice_number,
financial_administration as Market,
financial_administration,
financial_administration_id,

items_collection_method,
items_collection_date,

case when items_collection_method = 'delivery_date' then items_collection_date else null end as promised_delivery_date,

account_manager,
City,
user_category,

invoice_header_id,
invoice_header_printed_at, -- it can be null

case when date(invoice_header_printed_at) is not null then date(invoice_header_printed_at) else date(invoice_header_created_at) end as master_date,


date(invoice_header_printed_at) as date_invoice_header_printed_at,
PARSE_DATE('%Y-%m-%d', CONCAT(FORMAT_TIMESTAMP('%Y-%m', invoice_header_printed_at), '-01')) as year_month_invoice_header_printed_at,
date(invoice_header_created_at) as date_invoice_header_created_at,

invoice_header_status, --Draft,signed,Open,Printed,Closed,Canceled,Rejected,voided
invoice_header_type, --credit note, invoice
generation_type,
printed_by,



proof_of_delivery_id,
customer_id,

currency,

--fct
remaining_amount,
paid_amount,
total_amount_without_tax,
total_tax,
discount_amount,
price_without_discount,
total_amount,


current_timestamp() as insertion_timestamp 


from {{ref('int_invoices')}} as i
)

select * from source

where invoice_header_type = 'credit note'
with

source as ( 

select

number as invoice_number,
financial_administration as Market,
financial_administration,
financial_administration_id,

account_manager,
City,
client_category,

invoice_header_id,
invoice_header_printed_at, -- it can be null
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

--where invoice_header_printed_at is null
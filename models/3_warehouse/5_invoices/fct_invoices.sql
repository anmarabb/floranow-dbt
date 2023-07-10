with

source as ( 

select

number as invoice_number,
financial_administration as Market,
financial_administration,

invoice_header_id,
invoice_header_printed_at,
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





current_timestamp() as insertion_timestamp 


from {{ref('int_invoices')}} as i
)

select * from source

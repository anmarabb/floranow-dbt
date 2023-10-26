with

source as ( 

select

---Gross Revenue: This is the total amount of revenue generated from all printed invoices in a given period, without considering any adjustments like credit notes.
    gross_revenue,
    credit_note,
    invoice_count,
    credit_note_count,
    auto_gross_revenue,
    auto_credit_note,

    case when  date_diff(date(i.invoice_header_printed_at) , current_date() , MONTH) = 0 then gross_revenue else 0 end as mtd_gross_revenue,
    case when  date_diff(date(i.invoice_header_printed_at) , current_date() , MONTH) = 0 then credit_note else 0 end as mtd_credit_note,

    case when date_diff(current_date(),date(i.invoice_header_printed_at), MONTH) = 1 and extract(day FROM i.invoice_header_printed_at) <= extract(day FROM current_date()) then gross_revenue else 0 end as lmtd_gross_revenue,
    case when date_diff(current_date(),date(i.invoice_header_printed_at), MONTH) = 1 and extract(day FROM i.invoice_header_printed_at) <= extract(day FROM current_date()) then credit_note else 0 end as lmtd_credit_note,



case 
    when date_diff(current_date(), date(i.invoice_header_printed_at), YEAR) = 1 
    and extract(month FROM i.invoice_header_printed_at) = extract(month FROM current_date()) 
    and extract(day FROM i.invoice_header_printed_at) <= extract(day FROM current_date()) 
    then gross_revenue else 0 
end as lymtd_gross_revenue,


case 
    when date_diff(current_date(), date(i.invoice_header_printed_at), YEAR) = 1 
    and extract(month FROM i.invoice_header_printed_at) = extract(month FROM current_date()) 
    and extract(day FROM i.invoice_header_printed_at) <= extract(day FROM current_date()) 
    then credit_note else 0 
end as lymtd_credit_note,

registered_clients,

--invoice_items
    total_cost,
    invoice_items_count,
    quantity,


number as invoice_number,
financial_administration as Market,
financial_administration,
financial_administration_id,

items_collection_method,
items_collection_date,

delivery_date,

account_manager,
City,
client_category,
Customer,
debtor_number,
Warehouse,

invoice_header_id,
invoice_header_printed_at, -- it can be null

case when date(invoice_header_printed_at) is not null then date(invoice_header_printed_at) else date(invoice_header_created_at) end as master_date,


drop_id,

date(invoice_header_printed_at) as date_invoice_header_printed_at,
PARSE_DATE('%Y-%m-%d', CONCAT(FORMAT_TIMESTAMP('%Y-%m', invoice_header_printed_at), '-01')) as year_month_invoice_header_printed_at,
date(invoice_header_created_at) as date_invoice_header_created_at,
invoice_header_created_at,
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




--damged_value,


invoice_link,


line_items_count,


incidents_count,

invoice_items_detection,

line_items_detection,
full_detection,



DATE(
    EXTRACT(YEAR FROM invoice_header_printed_at),
    EXTRACT(MONTH FROM invoice_header_printed_at),
    1
) AS Year_Month_printed_at,


current_timestamp() as insertion_timestamp 


from {{ref('int_invoices')}} as i
)

select * from source


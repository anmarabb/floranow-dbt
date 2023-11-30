with

source as ( 

select
case when signed_at is not null then 1 else 0 end as signed_invoice,
case when invoice_header_printed_at is not null then 1 else 0 end as printed_invoice,

---Gross Revenue: This is the total amount of revenue generated from all printed invoices in a given period, without considering any adjustments like credit notes.
    gross_revenue,
    credit_note,
    invoice_count,
    credit_note_count,
    auto_gross_revenue,
    auto_credit_note,
    invoice_items_record_count,
    credit_note_items_count,

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


CASE 
    WHEN EXTRACT(YEAR FROM i.invoice_header_printed_at) = EXTRACT(YEAR FROM current_date())
    AND date(i.invoice_header_printed_at) <= current_date() THEN gross_revenue 
    ELSE 0 
END AS ytd_gross_revenue,

CASE 
    WHEN EXTRACT(YEAR FROM i.invoice_header_printed_at) = EXTRACT(YEAR FROM current_date())
    AND date(i.invoice_header_printed_at) <= current_date() THEN credit_note 
    ELSE 0 
END AS ytd_credit_note,

CASE 
    WHEN EXTRACT(YEAR FROM i.invoice_header_printed_at) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
    AND DATE(i.invoice_header_printed_at) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) THEN gross_revenue 
    ELSE 0 
END AS lytd_gross_revenue,


CASE 
    WHEN EXTRACT(YEAR FROM i.invoice_header_printed_at) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
    AND DATE(i.invoice_header_printed_at) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) THEN credit_note 
    ELSE 0 
END AS lytd_credit_note,




registered_clients,

--invoice_items
    total_cost,
    invoice_items_count,
    quantity,
    ii_gross_revenue,
    ii_credit_note,
    delivery_charge_amount,
    astra_gross_revenue,
    astra_credit_note,
    non_astra_gross_revenue,
    non_astra_credit_note,
    tbs_gross_revenue,
    tbs_credit_note,






invoice_number,
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
warehouse,

invoice_header_id,
invoice_header_printed_at, -- it can be null

case when date(invoice_header_printed_at) is not null then date(invoice_header_printed_at) else date(invoice_header_created_at) end as master_date,


drop_id,


signed_at,
date(invoice_header_printed_at) as date_invoice_header_printed_at,
PARSE_DATE('%Y-%m-%d', CONCAT(FORMAT_TIMESTAMP('%Y-%m', invoice_header_printed_at), '-01')) as year_month_invoice_header_printed_at,
PARSE_DATE('%Y-%m-%d', CONCAT(FORMAT_TIMESTAMP('%Y-%m', invoice_header_printed_at), '-01')) as month_printed_date,

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


company_id,
company_name,


payment_status,
payment_term,
source_system,
case when payment_status in ('Totally paid','Partially paid') then 1 else 0 end as collected_invoices_count,
case when payment_status in ('Not paid') then 1 else 0 end as not_collected_invoices_count,




   case when company_id = 3 then gross_revenue else 0 end as  bmx_gross_revenue,
  --  bmx_credit_note,

EXTRACT(month FROM invoice_header_printed_at) AS month,

CASE WHEN  invoice_header_printed_at >= '2022-01-01' AND invoice_header_printed_at < '2023-01-01' THEN gross_revenue ELSE 0 END AS gross_revenue_2022,
CASE WHEN  invoice_header_printed_at >= '2023-01-01' AND invoice_header_printed_at < '2024-01-01' THEN gross_revenue ELSE 0 END AS gross_revenue_2023,


current_timestamp() as insertion_timestamp 


from {{ref('int_invoices')}} as i
)

select * from source


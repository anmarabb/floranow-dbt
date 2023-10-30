
with 

invoice_items as (
    SELECT
    ii.invoice_header_id,
    sum(case when i.invoice_header_type = 'invoice' then ii.quantity * li.unit_landed_cost else 0 end)  as total_cost,
    count(ii.invoice_item_id) as invoice_items_count,
    --sum(ii.quantity) as quantity,
    sum(case when i.invoice_header_type != 'invoice' then -ii.quantity else ii.quantity end) as quantity,

    sum(case when invoice_header_type = 'invoice' and invoice_item_status = 'APPROVED' then ii.price_without_tax else 0 end) as ii_gross_revenue,
    sum(case when invoice_header_type = 'credit note' and invoice_item_status = 'APPROVED' then ii.price_without_tax else 0 end) as ii_credit_note,


    from {{ ref('stg_invoice_items') }} as ii 
    left join {{ ref('fct_order_items') }} as li on ii.line_item_id = li.line_item_id
    left join {{ ref('stg_invoices') }} as i on ii.invoice_header_id = i.invoice_header_id


    where ii.invoice_item_status = 'APPROVED' and ii.deleted_at is null
    group by ii.invoice_header_id
),

line_items as (
    select
    li.invoice_header_id,
    sum(li.incidents_count) as incidents_count,
    count(li.line_item_id) as line_items_count,
    from {{ ref('fct_order_items')}} as li
    group by 1
                
),

prep_payments as (
    select 
    py.invoice_header_id,
    sum(py.total_amount) as total_payments ,
    --sum(py.paid_amount) as paid_amount
    
    sum(py.credit_note_amount) as credit_note_amount_used,
    from {{ ref('int_payments') }} as py
    group by invoice_header_id
),

prep_move_item as (
    select 
    mi.documentable_id,
    mi.documentable_type, 
    mi.company_id

    from {{ ref('int_move_items') }} as mi
    left join {{ ref('stg_invoices')}} as i on i.invoice_header_id = mi.documentable_id and mi.documentable_type  = 'Invoice'
    where mi.deleted_at is null  and i.deleted_at is null

    group by 1,2,3
)



/*
,
prep_damaged as (
select
date (created_at) as date_incident_at,
Warehouse,
financial_administration,
case when  pi.incident_report = 'Inventory Dmaged' then pi.incident_quantity else 0 end as damged_quantity,
case when  pi.incident_report = 'Inventory Dmaged' then pi.incident_value else 0 end as damged_value,


from {{ref('int_product_incidents')}} as pi 

)
 */    

select     

i.*,


    case when invoice_header_type = 'invoice' and invoice_header_status in('Printed','signed')  then total_amount_without_tax else 0 end as gross_revenue,
    case when invoice_header_type = 'credit note' and invoice_header_status in('Printed','signed')  then total_amount_without_tax else 0 end as credit_note,

    case when invoice_header_type = 'invoice' and invoice_header_status in('Printed','signed')  then 1 else 0 end as invoice_count,
    case when invoice_header_type = 'credit note' and invoice_header_status in('Printed','signed')  then 1 else 0 end as credit_note_count,

    case when invoice_header_type = 'invoice' and invoice_header_status in('Printed','signed') and generation_type = 'AUTO' then total_amount_without_tax else 0 end as auto_gross_revenue,
    case when invoice_header_type = 'credit note' and invoice_header_status in('Printed','signed') and generation_type = 'AUTO' then total_amount_without_tax else 0 end as auto_credit_note,



concat(customer.debtor_number,i.items_collection_date) as drop_id, 



    printed_by.name as printed_by,

    customer.account_manager,
    customer.City,
    customer.user_category as client_category,
    customer.name as Customer,
    customer.Warehouse,
    customer.debtor_number,


    prep_payments.total_payments,
    prep_payments.credit_note_amount_used,

--invoice_items
    ii.total_cost,
    ii.invoice_items_count,
    ii.quantity,
    case when ii.invoice_items_count > 0 then 'With Invoice Items' else 'No Invoice Items' end as invoice_items_detection,
    ii.ii_gross_revenue,
    ii.ii_credit_note,


--line_items
    li.line_items_count,
    li.incidents_count,
    case when li.line_items_count > 0 then 'With Order Items' else 'No Order Items' end as line_items_detection,


case 
    when ii.invoice_items_count > 0 and li.line_items_count > 0 then 'Full Invoice Data: Invoice with both Invoice Items and Line Items' 
    when ii.invoice_items_count is null and li.line_items_count is null then 'Incomplete Invoice Data: Invoice without both Invoice Items and Line Items'
    when ii.invoice_items_count > 0 and  li.line_items_count is null then 'Invoice Without Line Items Details: Invoice with Invoice Items but without Line Items'
    when li.line_items_count > 0 and ii.invoice_items_count is null then 'Invoice Without Invoice Items Details: Invoice with Line Items but without Invoice Items'
end as full_detection,







concat( "https://erp.floranow.com/invoices/", i.invoice_header_id) as invoice_link,


case when items_collection_method = 'delivery_date' then items_collection_date else null end as delivery_date,


mi.company_id,

case 
when mi.company_id = 3 then 'Bloomax Flowers LTD'
when mi.company_id = 2 then 'Global Floral Arabia tr'
when mi.company_id = 1 then 'Flora Express Flower Trading LLC'
else  'To Be Scoped'
end as company_name,



fn.registered_clients,

    current_timestamp() as insertion_timestamp, 

from {{ ref('stg_invoices')}} as i
left join {{ ref('base_users') }} as printed_by on printed_by.id = i.printed_by_id
left join {{ ref('base_users') }} as customer on customer.id = i.customer_id
left join invoice_items as ii on ii.invoice_header_id = i.invoice_header_id
left join prep_payments as prep_payments on prep_payments.invoice_header_id = i.invoice_header_id
left join prep_move_item as mi on mi.documentable_id = i.invoice_header_id and mi.documentable_type  = 'Invoice'

left join line_items as li on li.invoice_header_id = i.invoice_header_id

left join  {{ ref('stg_financial_administrations') }} as fn on fn.id = i.financial_administration_id


--left join prep_damaged as prep_damaged on prep_damaged.date_incident_at = date(i.invoice_header_printed_at) and prep_damaged.Warehouse = customer.Warehouse and prep_damaged.financial_administration = i.financial_administration

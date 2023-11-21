-- fullfeled (added to loc, and genrate product_location recourd)
--

with

source as ( 

 
select 

    incident_at,
    order_date,
    delivery_date,
    departure_date,
    
    product_incident_id,
    line_item_id,
    incidentable_id, 
    credit_note_item_id,

    stem_length,
   
    
    incident_type,  --MISSING, EXTRA, DAMAGED, RETURNED
    stage,        --PACKING, RECEIVING, INVENTORY, DELIVERY, AFTER_RETURN
    incidentable_type, -- PackageLineItem, InvoiceItem, LineItem, ProductLocation, Product

    reported_by,

    accountable_type,
    Accountable,
    customer,
    Supplier,
    Origin,


    incident_report,

    incident_cost,
        extra_cost,
        incident_cost_without_extra,
        incident_cost_inventory_dmaged,

    incident_quantity,
        extra_quantity,
        incident_quantity_without_extra,
        incident_quantity_inventory_dmaged,
    
    incidents_count,
        extra_count,
        incidents_count_without_extra,
        incidents_count_inventory_dmaged,





    credited,



    
case when pi.stage in ('PACKING', 'RECEIVING')  then 'Pre Arrival' else 'Post Arrival' end as shipment_phase,

case 
    when pi.stage in ('PACKING', 'RECEIVING')  then 'S&L Team' 
    when pi.stage in ('INVENTORY')  then 'Fulfillment Team' 
    --when pi.stage in ('AFTER_RETURN', 'DELIVERY')  then 'Shared Responsibility To Be Scoped'
    when pi.stage in ('AFTER_RETURN', 'DELIVERY') and pi.incident_type in ('DAMAGED','MISSING') and Origin =  'Netherlands' then 'S&L Team'
    when pi.stage in ('AFTER_RETURN', 'DELIVERY') and pi.incident_type  in ('DAMAGED') and Origin !=  'Netherlands' then 'Fulfillment Team' 
    when pi.stage in ('AFTER_RETURN', 'DELIVERY') and pi.incident_type  in ('MISSING') and Origin !=  'Netherlands' then 'LMD Team' 
    when pi.incident_type  in ('TRANSACTIONAL_ISSUES')  then 'IT Team' 
    when pi.incident_type  in ('RETURNED','INCORRECT_ITEM','QUALITY_ISSUES') and Origin =  'Netherlands' then 'S&L Team'
    when pi.incident_type  in ('RETURNED','INCORRECT_ITEM','QUALITY_ISSUES') and Origin !=  'Netherlands' then 'Fulfillment Team'

 else 'Shared Responsibility To Be Scoped'

 end as responsible_team,


    --line_item

     warehouse,
     warehouse_country,
        

Stock,
stock_model_details,
stock_model,
full_stock_name,

NCR,


CONCAT(
CASE 
WHEN EXTRACT(ISOWEEK FROM incident_at) = 1 AND EXTRACT(MONTH FROM incident_at) = 12 THEN CAST(EXTRACT(YEAR FROM incident_at) + 1 AS STRING)
WHEN EXTRACT(ISOWEEK FROM incident_at) >= 52 AND EXTRACT(MONTH FROM incident_at) = 1 THEN CAST(EXTRACT(YEAR FROM incident_at) - 1 AS STRING)
ELSE CAST(EXTRACT(YEAR FROM incident_at) AS STRING)
END,
' - week ',
CAST(EXTRACT(ISOWEEK FROM incident_at) AS STRING)
) AS Year_Week_incident_at,


CONCAT(
CASE 
WHEN EXTRACT(ISOWEEK FROM order_date) = 1 AND EXTRACT(MONTH FROM order_date) = 12 THEN CAST(EXTRACT(YEAR FROM order_date) + 1 AS STRING)
WHEN EXTRACT(ISOWEEK FROM order_date) >= 52 AND EXTRACT(MONTH FROM order_date) = 1 THEN CAST(EXTRACT(YEAR FROM order_date) - 1 AS STRING)
ELSE CAST(EXTRACT(YEAR FROM order_date) AS STRING)
END,
' - week ',
CAST(EXTRACT(ISOWEEK FROM order_date) AS STRING)
) AS Year_Week_order_at,


CONCAT(
CASE 
WHEN EXTRACT(ISOWEEK FROM delivery_date) = 1 AND EXTRACT(MONTH FROM delivery_date) = 12 THEN CAST(EXTRACT(YEAR FROM delivery_date) + 1 AS STRING)
WHEN EXTRACT(ISOWEEK FROM delivery_date) >= 52 AND EXTRACT(MONTH FROM delivery_date) = 1 THEN CAST(EXTRACT(YEAR FROM delivery_date) - 1 AS STRING)
ELSE CAST(EXTRACT(YEAR FROM delivery_date) AS STRING)
END,
' - week ',
CAST(EXTRACT(ISOWEEK FROM delivery_date) AS STRING)
) AS Year_Week_delivery_date,


master_report_filter,

incidents_link,
financial_administration,
debtor_number,

currency,
fob_currency,


invoice_item_id,

after_sold,

pi_record_type,

state, --from line item
fulfillment_mode,
customer_id,

box_label,


--
    type_reason,
    reason,
    note,


order_type,


        product_category,
        product_subcategory,
        Product,
        line_item_link,

master_shipment,
Shipment,

fob_value,


Reseller,
li_record_type_details,
li_record_type,
order_source,


reseller_type,
current_timestamp() as insertion_timestamp, 


from {{ref('int_product_incidents')}} as pi 

where pi.customer_id not in (1289,1470,2816,11123)
)

select * from source


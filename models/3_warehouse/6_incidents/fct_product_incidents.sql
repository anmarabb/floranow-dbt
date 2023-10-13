-- fullfeled (added to loc, and genrate product_location recourd)
--

with

source as ( 

 
select 

    incident_at,
    order_date,
    delivery_date,
    
    product_incident_id,
    line_item_id,
    incidentable_id, 
    credit_note_item_id,

    
    raw_incident_quantity, --- = incident_quantity + ABS(extra_quantity)
    incident_quantity,
    extra_quantity,
   
    
    incident_type,  --MISSING, EXTRA, DAMAGED, RETURNED
    stage,        --PACKING, RECEIVING, INVENTORY, DELIVERY, AFTER_RETURN
    incidentable_type, -- PackageLineItem, InvoiceItem, LineItem, ProductLocation, Product

    reported_by,

    accountable_type,
    Accountable,
    customer,
    Supplier,


    incident_report,
    incident_value,


    credited,



    
case when pi.stage in ('PACKING', 'RECEIVING')  then 'Pre Shipment' else 'Post Shipment' end as shipment_phase,


    --line_item

     warehouse,
        
        

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

currency,


invoice_item_id,

after_sold,

pi_record_type,

state, --from line item


current_timestamp() as insertion_timestamp, 


from {{ref('int_product_incidents')}} as pi 

)

select * from source


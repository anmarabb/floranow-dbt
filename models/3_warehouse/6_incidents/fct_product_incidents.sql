-- fullfeled (added to loc, and genrate product_location recourd)
--

with

source as ( 

 
select 

    incident_at,
    
    product_incident_id,
    line_item_id,
    incidentable_id, 
    credit_note_item_id,

    

    incident_quantity,
   
    
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


    


    --line_item

    --Warehouse,
        
        

CONCAT(
CASE 
WHEN EXTRACT(ISOWEEK FROM incident_at) = 1 AND EXTRACT(MONTH FROM incident_at) = 12 THEN CAST(EXTRACT(YEAR FROM incident_at) + 1 AS STRING)
WHEN EXTRACT(ISOWEEK FROM incident_at) >= 52 AND EXTRACT(MONTH FROM incident_at) = 1 THEN CAST(EXTRACT(YEAR FROM incident_at) - 1 AS STRING)
ELSE CAST(EXTRACT(YEAR FROM incident_at) AS STRING)
END,
' - week ',
CAST(EXTRACT(ISOWEEK FROM incident_at) AS STRING)
) AS `Year Week`,


master_report_filter,

incidents_link,
financial_administration,

currency,


current_timestamp() as insertion_timestamp, 


from {{ref('int_product_incidents')}} as pi 

)

select * from source


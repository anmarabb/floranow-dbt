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


    


    --line_item

    --Warehouse,
        
        


current_timestamp() as insertion_timestamp, 

from {{ref('int_product_incidents')}} as pi 

)

select * from source


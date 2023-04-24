With source as (
 select * from {{ source('erp_prod', 'product_incidents') }}
)
select 
            --PK
                pi.id as product_incident_id,
            --FK
                line_item_id,
                incidentable_id,             -- id for incidentable type ( line item id, package line item id, invoice item id ..etc)
                credit_note_item_id,         -- credit note id for this incident if the incidentable type is InvoiceItem
                accountable_id,
                location_id,                 -- What is the location you are reporting from? ( if the incident during inventory stage )
                inventory_cycle_check_id,    -- cycle count id if the incident reported from cycle count


            --dim
                --date
                created_at,
                deleted_at,
                updated_at,

            incident_type,     -- MISSING, EXTRA, DAMAGED, RETURNED
            incidentable_type, -- PackageLineItem, InvoiceItem, LineItem, ProductLocation, Product
            accountable_type,
            stage,
            reported_by as reported_by_id ,
            credited, -- false: (no credit note for this incident, default false ), true: (there is credit note for this incident)
            after_sold, -- false: (report incidents on line item or product before sold the product), true: (report incident on product after sold all quantity for this product)
            status,
            note,




            --fct
            quantity,
            valid_quantity,
            accounted_quantity,


            case 
                when incident_type='EXTRA'  then 'Extra'
                else 'Incident'
                end as record_type,


        case 
            when stage = 'INVENTORY' and incident_type = 'DAMAGED'  then 'Inventory Dmaged'
            when stage = 'INVENTORY' and incident_type in ('MISSING', 'RETURNED')  then 'Inventory Incidents'
            when stage in ('PACKING', 'RECEIVING') then 'Supplier Incidents'
            else null
            end as incident_report,


    
current_timestamp() as ingestion_timestamp,




from source as pi


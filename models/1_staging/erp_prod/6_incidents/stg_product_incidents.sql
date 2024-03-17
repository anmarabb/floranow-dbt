With source as (
 select * from {{ source(var('erp_source'), 'product_incidents') }}
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
                created_at as incident_at,
                deleted_at,
                updated_at,

            incident_type,     
                               -- MISSING
                               -- EXTRA
                               -- DAMAGED
                               -- RETURNED        The line item contains a returned quantity (  return the line item from customer )
                               -- QUALITY_ISSUES
                               -- TRANSACTIONAL_ISSUES
                               -- BEFORE_SUPPLY: used when reporting an incident in the origin warehouse, which we consider it as supplier side in inner shipment between warehouses.




            incidentable_type,
                                 -- PackageLineItem:   Report incidents on package line items ( packing and receiving stage)
                                 -- InvoiceItem:       Report incidents on invoice item ( after delivery the line item )
                                 -- LineItem:          Report incidents on line item ( incidents on child line items )
                                 -- ProductLocation:   Report incidents on product location ( the line item in warehouse )
                                 -- Product:           Report incidents on product ( after sold the product )




            accountable_type,

            stage,  
                    -- PACKING:        report incidents during packing stage
                    -- RECEVING:       report incidents during receiving stage 
                    -- INVENTORY:      report incidents during inventory stage ( line item in the warehouse )
                    -- AFTER RETURN:   report incidents after the line item returned from customer
                    -- DELIVERY:       report incidents during delivery stage 




            reported_by as reported_by_id,

            credited, 
                      -- false: (no credit note for this incident, default false ), 
                      -- true: (there is credit note for this incident)


            after_sold, 
                      -- false: (report incidents on line item or product before sold the product), 
                      -- true: (report incident on product after sold all quantity for this product)



            status, --REPORTED, CLOSED, null | It is no longer used (dev team)
            note,
            reason,




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


            case 
                when pi.stage in ('PACKING', 'RECEIVING') then 'supplier_incidents'
                when pi.stage = 'DELIVERY' then 'DELIVERY'
                when pi.stage = 'AFTER_RETURN' then 'AFTER_RETURN'
                when pi.stage = 'INVENTORY' and pi.incident_type = 'DAMAGED'  then 'inventory_dmaged'
                when pi.stage = 'INVENTORY' and pi.incident_type != 'DAMAGED'  then 'inventory_incidents'
                else null  
            end as master_report_filter,


    
current_timestamp() as ingestion_timestamp,




from source as pi
where  pi.deleted_at is null

and reported_by != 10988


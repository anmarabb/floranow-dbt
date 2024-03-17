

select 

            --PK
                id as master_shipment_id,
            --FK
                warehouse_id,
                customer_id,

                order_sequence, --Generated number for the line items (prefix for each master shipment).



            --date
                created_at,
                updated_at,
                departure_date,
                canceled_at,
                deleted_at,
                arrival_time as arrival_at, --i think this is when the team click the open butomn
                --Manually entered arrival time at the last destination (warehouse) when the master shipment is open all shipments in the master shipment are packed.


                case when origin in ('CO','NL') then departure_date + 1 else departure_date  end as arrival_date,


                



            --dim
                name as master_shipment,
                destination, --Warehouse name.
                origin, --Original country of the master shipment.



                
                
                customer_type,  

                status as master_shipments_status, 
                    --DRAFT:(initial state)  All or part of the shipments in the master shipment are in draft form.
                    --PACKED: (pack all shipments) All or part of the shipments in the master shipment are packed.
                    --OPENED: (open manual)
                    --INSPECTED: (receive all shipments)
                    --WAREHOUSED: (all shipments warehoused) All or part of the shipments in the master shipment are warehoused.
                    --CANCELED: (cancel all shipments) Entire master shipment is canceled. 
                    --MISSING: All shipments in the master shipment are missing.
                    --CLOSED: 

                    --The status remains in draft if it is partially packed, partially warehoused, partially received, or partially canceled.
                    --Status for the shipment reflects the  master shipment.

                    
                    



                fulfillment as master_shipments_fulfillment_status, 
                    --UNACCOUNTED: No action taken after the master shipment is created.
                    --PARTIAL: Partial fulfillment of the master shipment. 
                    --SUCCEED: Successful fulfillment of all shipments in the master shipment.



               

                
                note,

                freight_currency,
                master_invoice_currency,
                clearance_currency,
                cancellation_reason,
                case when msh.customer_id is not null then 'Bulk shipments' else null end as shipment_type,
                concat( "https://erp.floranow.com/master_shipments/", msh.id) as master_shipment_link,



            --fct
                total_quantity as master_total_quantity, --Summation of all line item quantities in all shipments in the master shipment.
                clearance_cost, --Customs clearance cost for the shipment.
                master_invoice_cost, --Summation of the prices of shipments in the master shipment, converted into one currency.
                freight_cost, --Cost for support provided by the consolidation hub.
                total_fob, --Total supplier shipment prices with currency (represented as a dictionary).
            
            











from {{ source(var('erp_source'), 'master_shipments') }} as msh

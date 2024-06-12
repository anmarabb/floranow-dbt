With source as (
 select * from {{ source(var('erp_source'), 'shipments') }}
)
select 

            --PK
                id as shipment_id,
            --FK
                supplier_id,
                warehouse_id,
                customer_id,
                master_shipment_id,
                origin_warehouse_id,

                sent_by_id,

                packing_list_id,
                case when origin_warehouse_id is not null then 'inner shipment' else 'normal shipmnet' end as  shipment_type,
                -- inner shipment, which is shipment between warehouses

                number, --System-generated barcode for each shipment.



            --dim
                name as Shipment,
                status as shipments_status, 
                    --DRAFT: (initial state) Shipment is in draft state upon creation. 
                    --PACKED: (pack all package line items) Shipment is considered packed when flowers are manually packed into packages, each with a specified flower count.
                    --INSPECTED: (receive all package line items) one or more package in the shipment received thus it will be inspected
                    --WAREHOUSED: (add all package line items to location) if it received by client then once the shipment received it will be reflected as warehoused, else if it received by box thus once its released into POD or LOC it will be documented as warehoused
                    --MISSING: Line items are nonexistent on the farm, documented as missing for replacement.
                    --CANCELED: (cancel the shipment or all line items on the shipment) the line items of the shipment are not existed in the farm, or cancelled for any reason. thus the line items cancelled and their quantity in the line item table will be zero.


                fulfillment as shipments_fulfillment_status, 
                    --UNACCOUNTED: Shipment is unaccounted for, with no action taken after creation.
                    --PARTIAL: Partial fulfillment of the shipment. 
                    --SUCCEED: Successful fulfillment of the entire shipment.
                    --FAILED: Full incidents occurred during fulfillment.


                receiving_way,  
                    --BY_BOX: Verification of the number of stems in each box upon receipt from the supplier.
                    --BY_CLIENT:Standard distribution of boxes into orders for each client with quantity verification.
                    --null: Previous data before adding this column lacks any information.


                concat( "https://erp.floranow.com/shipments/", sh.id) as shipment_link,



               -- packing_type, --null
                --customer_type, --null
                
                cancellation_reason, --null
                

                note, --null
                previous_masters,
                



                invoice_uploaded_by,
                --proforma_uploaded_by,
                canceled_by_id,



            --date
               -- received_at,
                created_at, --Date and time generated upon shipment creation.
                updated_at,
                departure_date, --Supplier's estimated departure date

                
                canceled_at,
                deleted_at,

            --fct
                total_quantity as supplier_shipment_total_quantity,                     --Summation of line item quantities in each shipment.
                total_received_quantity as supplier_shipment_total_received_quantity,   --Summation of received quantities in the warehouse for each shipment.
                total_missing_quantity as supplier_shipment_total_missing_quantity,     --Summation of missed quantities during packing or receiving.
                total_damaged_quantity as supplier_shipment_total_damaged_quantity,     --Summation of damaged quantities during receiving.

                total_fob, --Sum of line item prices in the shipment, where FOB represents the farm price before shipping and additional costs.
                total_received_fob, --Sum of received line item prices in the shipment.
                total_missing_fob, --Sum of missing line item prices in the shipment.
                total_damaged_fob, --Sum of damaged line item prices in the shipment.
                
                invoice_amount, --Invoice from the supplier for the shipment.
                --proforma_amount, --Draft invoice sent to the S&L team for approval before printing.

                shipping_boxes_count, --Count of boxes distributed in the consolidation hub to facilitate shipping or partitioning during shipment.
                warehousing_boxes_count, --Number of boxes received in the warehouse.



    
is_sent,
is_local,


current_timestamp() as ingestion_timestamp,
 




from source as sh
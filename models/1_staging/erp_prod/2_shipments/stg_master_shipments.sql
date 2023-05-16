With source as (
 select * from {{ source('erp_prod', 'master_shipments') }}
)
select 

            --PK
                id as master_shipment_id,
            --FK
                warehouse_id,
                customer_id,



            --date
                created_at,
                updated_at,
                departure_date,
                canceled_at,
                deleted_at,
                arrival_time as arrival_at, --i think this is when the team click the open butomn



            --dim
                destination,
                
                total_fob,
                customer_type,  

                status as master_shipments_status,
                name as master_shipment_name,
                fulfillment as master_shipments_fulfillment_status, --UNACCOUNTED, PARTIAL, SUCCEED

                origin,
                order_sequence,
                note,

                freight_currency,
                master_invoice_currency,
                clearance_currency,
                cancellation_reason,
                case when msh.customer_id is not null then 'Bulk shipments' else null end as shipment_type,
                concat( "https://erp.floranow.com/master_shipments/", msh.id) as master_shipment_link,



            --fct
                total_quantity,
                clearance_cost,
                master_invoice_cost,
                freight_cost,
            
            







current_timestamp() as ingestion_timestamp, 




from source as msh
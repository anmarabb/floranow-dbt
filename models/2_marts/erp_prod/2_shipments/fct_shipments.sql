with

source as ( 

 
select

--Supplier Shipments
    --dim
        invoice_amount,
        Shipment,
        shipment_id,
        shipment_link,
        shipments_status, --DRAFT, PACKED, WAREHOUSED, CANCELED, MISSING
        shipments_fulfillment_status,

        Supplier,
        Origin,
        account_manager,
        




    --date
        created_at,
        

    --fct
        -- supplier_shipment_total_quantity,
        -- supplier_shipment_total_received_quantity,
        -- supplier_shipment_total_missing_quantity,
        -- supplier_shipment_total_damaged_quantity,
        shipping_boxes_count,
        warehousing_boxes_count,
        -- total_fob,

        total_quantity,
        total_fob,
        missing_quantity,
        missing_fob,
        damaged_quantity,
        damaged_fob,
        received_quantity,
        received_fob,





       -- case when shipments_status not in  ('CANCELED','DRAFT' ) then supplier_shipment_total_quantity else 0 end as expected_quantity,
       -- case when shipments_status in  ('CANCELED','DRAFT' ) then supplier_shipment_total_quantity else 0 end as not_expected_quantity,





--Master Shipments
    --dim
        master_shipment_link,
        master_shipment_id,
        master_shipments_status, --DRAFT, PACKED, OPENED, WAREHOUSED, CANCELED, MISSING, INSPECTED
        master_shipments_fulfillment_status,
        master_shipment,
        warehouse, --destination
        Destination,



    --date
        arrival_at,
        departure_date,
        arrival_date,
        select_arrival_date,


    --fct
        master_total_quantity,
        
















    



case when arrival_at is null and master_shipments_status in ('DRAFT','CANCELED', 'PACKED') then 'Not Arrived' else 'Arrived' end as arrival_status,

from {{ref('int_shipments')}} as sh 
)

select * from source

--where  date_diff(cast(current_date() as date ),cast(departure_date as date), Year) = 0
--where shipment_id= 30648


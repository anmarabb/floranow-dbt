with

source as ( 

 
select
master_shipment,
Shipment,
Supplier,
Origin,
warehouse, --destination
Destination,

shipment_link,
master_shipment_link,


account_manager,
shipment_id,
master_shipment_id,


--date
    created_at,
    departure_date,
    arrival_date,
    arrival_at,
    select_arrival_date,

shipments_status, --DRAFT, PACKED, WAREHOUSED, CANCELED, MISSING
shipments_fulfillment_status,


master_shipments_status, --DRAFT, PACKED, OPENED, WAREHOUSED, CANCELED, MISSING, INSPECTED
master_shipments_fulfillment_status,


--fct


case 
when arrival_at is null and master_shipments_status in ('DRAFT','CANCELED', 'PACKED') then 'Not Arrived'
else 'Arrived' end as arrival_status,

from {{ref('int_shipments')}} as sh 
)

select * from source

--where  date_diff(cast(current_date() as date ),cast(departure_date as date), Year) = 0

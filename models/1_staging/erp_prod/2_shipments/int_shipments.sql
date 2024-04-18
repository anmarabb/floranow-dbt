
    
 select 
 


--(pli.received_quantity - li.expected_quantity) * pli.fob_price as value_variance,


sh.* EXCEPT(ingestion_timestamp,master_shipment_id,departure_date),


msh.master_shipments_status,
msh.master_shipment,
msh.master_shipments_fulfillment_status,
msh.arrival_at,
msh.master_shipment_id,
msh.master_total_quantity,
msh.departure_date,
msh.arrival_date,
concat( "https://erp.floranow.com/master_shipments/", msh.master_shipment_id) as master_shipment_link,


shipments_suppliers.supplier_name as Supplier,
shipments_suppliers.supplier_region as Origin,
shipments_suppliers.account_manager,


w.warehouse_name as warehouse,
w.warehouse_country as Destination,


case when msh.arrival_at is not null then 1 else 0 end as shipments_received,
case when msh.arrival_at is null  then 'shipmnet_not_arrived' else 'shipmnet_arrived' end as shipmnet_arrival,



case 
    when date_diff(date(msh.arrival_date)  ,current_date(), month) > 1 then 'Wrong date' 
    when date(msh.arrival_date) = current_date()+1 then "Tomorrow" 
    when date(msh.arrival_date) > current_date() then "Future" 
    when date(msh.arrival_date) = current_date()-1 then "Yesterday" 
    when date(msh.arrival_date) = current_date() then "Today" 
    when date_diff(cast(current_date() as date ),cast(msh.arrival_date as date), MONTH) = 0 then 'Month To Date'
    when date_diff(cast(current_date() as date ),cast(msh.arrival_date as date), MONTH) = 1 then 'Last Month'
    when date_diff(cast(current_date() as date ),cast(msh.arrival_date as date), YEAR) = 0 then 'Year To Date'
    else "Past" end as select_arrival_date,

 


 

    current_timestamp() as ingestion_timestamp,


from {{ ref('stg_shipments') }} as sh
left join  {{ ref('stg_master_shipments') }} as msh on sh.master_shipment_id = msh.master_shipment_id
left join  {{ ref('base_warehouses') }} as w on msh.warehouse_id = w.warehouse_id
left join  {{ ref('base_suppliers') }} as shipments_suppliers on shipments_suppliers.supplier_id = sh.supplier_id




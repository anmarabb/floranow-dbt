with shipment_details as (
    select li.shipment_id,
           SUM(li.ordered_quantity - li.splitted_quantity - COALESCE(temp_pi.quantity, 0)) AS total_quantity,
           SUM((li.ordered_quantity - li.splitted_quantity - COALESCE(temp_pi.quantity, 0)) * li.raw_unit_fob_price) AS total_fob,
           COALESCE(SUM(pli.missing_quantity), 0) AS missing_quantity,
           SUM(COALESCE(pli.missing_quantity, 0) * li.raw_unit_fob_price) AS missing_fob,
           COALESCE(SUM(pli.damaged_quantity), 0) AS damaged_quantity,
           SUM(COALESCE(pli.damaged_quantity, 0) * li.raw_unit_fob_price) AS damaged_fob,
           COALESCE(SUM(pli.fulfilled_quantity), 0) AS received_quantity,
           SUM(COALESCE(pli.fulfilled_quantity, 0) * li.raw_unit_fob_price) AS received_fob,
           SUM(COALESCE(li.requested_quantity, 0)) AS requested_quantity,

    from {{ref('int_line_items')}} li
    left join {{ref('stg_package_line_items')}} pli on li.line_item_id = pli.line_item_id
    left join
    (
        SELECT
            SUM(pi.quantity) AS quantity,
            pi.line_item_id
        FROM
            {{ref("stg_product_incidents")}} pi
        WHERE
            pi.stage = 'BEFORE_SUPPLY'
        GROUP BY
            pi.line_item_id
    ) AS temp_pi ON temp_pi.line_item_id = li.line_item_id
    
    group by shipment_id

)
    
 select 
 


--(pli.received_quantity - li.expected_quantity) * pli.fob_price as value_variance,


sh.* EXCEPT(ingestion_timestamp,master_shipment_id,departure_date, supplier_shipment_total_quantity, supplier_shipment_total_received_quantity, supplier_shipment_total_missing_quantity, supplier_shipment_total_damaged_quantity, total_fob, total_received_fob, total_missing_fob, total_damaged_fob),


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

 
    sd.total_quantity,
    sd.total_fob,
    sd.missing_quantity,
    sd.missing_fob,
    sd.damaged_quantity,
    sd.damaged_fob,
    sd.received_quantity,
    sd.received_fob,
    sd.requested_quantity,
 

    current_timestamp() as ingestion_timestamp,


from {{ ref('stg_shipments') }} as sh
left join  {{ ref('stg_master_shipments') }} as msh on sh.master_shipment_id = msh.master_shipment_id
left join  {{ ref('base_warehouses') }} as w on msh.warehouse_id = w.warehouse_id
left join  {{ ref('base_suppliers') }} as shipments_suppliers on shipments_suppliers.supplier_id = sh.supplier_id
left join shipment_details as sd on sh.shipment_id = sd.shipment_id 



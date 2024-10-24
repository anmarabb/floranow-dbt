WITH shipment_details as (
  with package_line_items as (
      SELECT line_item_id,
             sum(quantity) as quantity,
             sum(fulfilled_quantity) as fulfilled_quantity,
             sum(damaged_quantity) as damaged_quantity,
      FROM {{ref("stg_package_line_items")}}
      GROUP BY 1
    ), 
        product_incident as (
        SELECT pi.line_item_id,
               sum(case when pi.stage = 'BEFORE_SUPPLY' then pi.quantity end) AS quantity,
               sum(case when pi.stage = 'PACKING' and pi.incident_type = 'MISSING' then pi.quantity end) AS missing_packing_quantity,
               sum(case when pi.stage = 'RECEIVING' and after_sold = False then pi.quantity end) AS incident_quantity_receiving_stage,
               sum(case when pi.stage = 'RECEIVING' and after_sold = False and pi.incident_type = 'MISSING' then pi.quantity end) AS missing_quantity_receiving_stage,
               sum(case when pi.stage = 'RECEIVING' and after_sold = False and pi.incident_type = 'DAMAGED' then pi.quantity end) AS damaged_quantity_receiving_stage,
               sum(case when pi.stage = 'RECEIVING' and after_sold = False and pi.incident_type = 'EXTRA' then pi.quantity end) AS extra_quantity_receiving_stage,
        FROM {{ref("stg_product_incidents")}} pi

        GROUP BY pi.line_item_id
    ) 
    

  SELECT li.shipment_id,
         sum(case when li.order_type not in ('ADDITIONAL', 'EXTRA') then li.ordered_quantity - li.splitted_quantity - COALESCE(temp_pi.quantity, 0) end) AS total_quantity,
    
         COALESCE(SUM(pli.damaged_quantity), 0) AS damaged_packing_quantity,
         COALESCE(SUM(pli.fulfilled_quantity), 0) AS received_quantity,
         sum(case when li.order_type not in ('ADDITIONAL', 'EXTRA') then COALESCE(li.requested_quantity, 0) end) AS requested_quantity,
         SUM(CASE WHEN pli.quantity > 0 and li.order_type not in ('ADDITIONAL', 'EXTRA') THEN COALESCE(pli.quantity, 0) - COALESCE(temp_pi.missing_packing_quantity, 0) END) AS packed_quantity,


         SUM(COALESCE(temp_pi.missing_packing_quantity, 0)) AS missing_packing_quantity,
         SUM(COALESCE(temp_pi.incident_quantity_receiving_stage, 0)) AS incident_quantity_receiving_stage,
         SUM(COALESCE(temp_pi.missing_quantity_receiving_stage, 0)) AS missing_quantity_receiving_stage,
         SUM(COALESCE(temp_pi.damaged_quantity_receiving_stage, 0)) AS damaged_quantity_receiving_stage,
         SUM(COALESCE(temp_pi.extra_quantity_receiving_stage, 0)) AS extra_quantity_receiving_stage,

         SUM(case when ad.creation_stage = 'PACKING' then ad.quantity END) AS packing_additional_quantity,

         SUM(case when li.order_type = 'EXTRA' and li.creation_stage = 'PACKING' then ordered_quantity END) as extra_packing_quantity,

  FROM {{ref("int_line_items")}} li
  LEFT JOIN package_line_items pli on li.line_item_id = pli.line_item_id
  LEFT JOIN {{ref("stg_additional_items_reports")}} ad on ad.line_item_id=li.line_item_id
  LEFT JOIN product_incident temp_pi on  li.line_item_id = temp_pi.line_item_id 
  GROUP BY shipment_id
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
    
    sd.damaged_packing_quantity,
    sd.received_quantity,
    sd.requested_quantity,
    sd.packed_quantity,


    sd.missing_packing_quantity,
    sd.incident_quantity_receiving_stage,
    sd.missing_quantity_receiving_stage,
    sd.damaged_quantity_receiving_stage,
    sd.extra_quantity_receiving_stage,

    sd.packing_additional_quantity,

    sd.extra_packing_quantity,
 

    current_timestamp() as ingestion_timestamp,


from {{ ref('stg_shipments') }} as sh
left join  {{ ref('stg_master_shipments') }} as msh on sh.master_shipment_id = msh.master_shipment_id
left join  {{ ref('base_warehouses') }} as w on msh.warehouse_id = w.warehouse_id
left join  {{ ref('base_suppliers') }} as shipments_suppliers on shipments_suppliers.supplier_id = sh.supplier_id
left join shipment_details as sd on sh.shipment_id = sd.shipment_id 



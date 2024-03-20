with lineItems as 
     (

        select
        li.shipment_id,
        SUM(li.ordered_quantity - li.splitted_quantity - li.incident_quantity_before_supply_stage) AS expected_quantity,
        SUM((li.ordered_quantity - li.splitted_quantity - li.incident_quantity_before_supply_stage) * li.unit_fob_price) AS expected_fob,
        count(li.line_item_id) as line_items_count,

        from {{ ref('fct_order_items') }} as li
        --where shipment_id = 31174
        group by 1

      ),


packageLineItems as 
    (
        select
        pli.shipment_id,
        COALESCE(SUM(pli.missing_quantity), 0) AS missing_quantity,
        SUM(COALESCE(pli.missing_quantity, 0) * pli.raw_unit_fob_price) AS missing_fob,
        COALESCE(SUM(pli.damaged_quantity), 0) AS damaged_quantity,
        SUM(COALESCE(pli.damaged_quantity, 0) * pli.raw_unit_fob_price) AS damaged_fob,
        COALESCE(SUM(pli.fulfilled_quantity), 0) AS received_quantity,
        SUM(COALESCE(pli.fulfilled_quantity, 0) * pli.raw_unit_fob_price) AS received_fob,


        from  {{ ref('int_package_line_items') }} as pli

        --left join lineItems as li on li.line_item_id = pli.line_item_id

        group by pli.shipment_id
    )
    
 select 
 
(pli.received_quantity - li.expected_quantity) as shipment_quantity_variance,
(pli.received_fob - li.expected_fob) as shipment_value_variance,


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

 

li.expected_quantity,
li.expected_fob,
li.line_items_count,

pli.missing_quantity,
pli.missing_fob,

pli.damaged_quantity,
pli.damaged_fob,

pli.received_quantity,
pli.received_fob,

 

    current_timestamp() as ingestion_timestamp,


from {{ ref('stg_shipments') }} as sh
left join  {{ ref('stg_master_shipments') }} as msh on sh.master_shipment_id = msh.master_shipment_id
left join  {{ ref('base_warehouses') }} as w on msh.warehouse_id = w.warehouse_id
left join  {{ ref('base_suppliers') }} as shipments_suppliers on shipments_suppliers.supplier_id = sh.supplier_id

left join  lineItems as li on sh.shipment_id = li.shipment_id


LEFT JOIN packageLineItems AS pli ON sh.shipment_id = pli.shipment_id



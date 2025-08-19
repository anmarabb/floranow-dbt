with 

productIncidents as 
        
      (

        select 
        pi.line_item_id,
        concat( "https://erp.floranow.com/line_items/", pi.line_item_id) as line_item_link,
        count(*) as incidents_count,
        count(case when incident_type !='EXTRA'  then 1 else null end) as incidents_count_without_extra,
        count(case when incident_type ='EXTRA'  then 1 else null end) as extra_count,
        count(case when pi.stage = 'INVENTORY' and pi.incident_type = 'DAMAGED' then 1 else null end) as incidents_count_inventory_dmaged,
        count(case when pi.stage = 'INVENTORY' and pi.incident_type = 'DAMAGED' OR incident_type ='EXTRA' then null else 1 end) as incidents_count_without_extra_without_inventory_dmaged,
        sum(pi.quantity) as incident_quantity,
        sum(case when incident_type !='EXTRA'  then pi.quantity else 0 end) as incident_quantity_without_extra,
        sum(case when incident_type ='EXTRA'  then  pi.quantity else 0 end) as extra_quantity,
        sum(case when pi.stage = 'INVENTORY' and pi.incident_type = 'DAMAGED' then pi.quantity else 0 end) as incident_quantity_inventory_dmaged,
        sum(case when incident_type !='EXTRA'  and pi.stage = 'RECEIVING' then  pi.quantity else 0 end) as incident_quantity_receiving_stage,
        sum(case when incident_type !='EXTRA'  and pi.stage = 'PACKING' then  pi.quantity else 0 end) as incident_quantity_packing_stage,
        sum(case when incident_type not in ('DAMAGED','EXTRA') and pi.stage = 'INVENTORY'  then pi.quantity else 0 end) as incident_quantity_inventory_stage,
        sum(case when incident_type !='EXTRA' and pi.stage = 'DELIVERY'  then pi.quantity else 0 end) as incident_quantity_delivery_stage,
        sum(case when incident_type !='EXTRA' and pi.stage = 'AFTER_RETURN'  then pi.quantity else 0 end) as incident_quantity_after_return_stage,
        sum(case when pi.stage = 'BEFORE_SUPPLY' then  pi.quantity else 0 end) as incident_quantity_before_supply_stage,
        max(case when incident_type !='EXTRA'  and pi.stage = 'RECEIVING' then  li.order_number else null end) as incident_orders_receiving_stage,
        max(case when incident_type !='EXTRA'  and pi.stage = 'PACKING' then  li.order_number else null end) as incident_orders_packing_stage,
        max(case when incident_type not in ('DAMAGED','EXTRA') and pi.stage = 'INVENTORY'  then li.order_number else null end) as incident_orders_inventory_stage,
        max(case when incident_type !='EXTRA' and pi.stage = 'DELIVERY'  then li.order_number else null end) as incident_orders_delivery_stage,
        max(case when incident_type !='EXTRA' and pi.stage = 'AFTER_RETURN'  then li.order_number else null end) as incident_orders_after_return_stage,
        sum( pi.quantity * li.unit_landed_cost ) as incident_cost,
        sum(case when incident_type !='EXTRA'  then pi.quantity * li.unit_landed_cost else 0 end) as incident_cost_without_extra,
        sum(case when incident_type ='EXTRA'  then pi.quantity * li.unit_landed_cost else 0 end) as extra_cost,
        sum(case when pi.stage = 'INVENTORY' and pi.incident_type = 'DAMAGED' then pi.quantity * li.unit_landed_cost else 0 end) as incident_cost_inventory_dmaged,
        sum(case when incident_type ='EXTRA' and pi.stage = 'PACKING' then  pi.quantity else 0 end) as incident_quantity_extra_packing,
        sum(case when incident_type ='EXTRA' and pi.stage = 'RECEIVING' then  pi.quantity else 0 end) as incident_quantity_extra_receiving,
        sum(case when incident_type ='EXTRA' and pi.stage = 'INVENTORY' then  pi.quantity else 0 end) as incident_quantity_extra_inventory,
        sum(case when after_sold is false and pi.stage = 'INVENTORY' and incident_type ='MISSING' then pi.quantity else 0 end) as inventory_missing_quantity,

       sum(case when pi.stage = 'PACKING' and pi.incident_type = 'MISSING' then pi.quantity end) AS missing_packing_quantity,
       sum(case when pi.stage = 'RECEIVING' and after_sold = False then pi.quantity end) AS sh_incident_quantity_receiving_stage,
       sum(case when pi.stage = 'RECEIVING' and after_sold = False and pi.incident_type = 'MISSING' then pi.quantity end) AS missing_quantity_receiving_stage,
       sum(case when pi.stage = 'RECEIVING' and after_sold = False and pi.incident_type = 'DAMAGED' then pi.quantity end) AS damaged_quantity_receiving_stage,
       sum(case when pi.stage = 'RECEIVING' and after_sold = False and pi.incident_type = 'EXTRA' then pi.quantity end) AS extra_quantity_receiving_stage,
       sum(case when pi.incidentable_type = 'ProductLocation' and  pi.incident_type != 'EXTRA' then pi.quantity end) AS incident_quantity_in_warehouse,
        
        from {{ ref('stg_product_incidents') }} as pi  
        left join {{ref('stg_line_items')}} as li on pi.line_item_id = li.line_item_id
        where li.customer_id not in (1289,1470,2816,11123)
        group by pi.line_item_id

      ),

PackageLineItems as 
      (
        select 
        line_item_id, 
        sum(quantity) as packed_quantity, --Packed Qty.
        sum(fulfilled_quantity) as pli_fulfilled_quantity,
        SUM(missing_quantity) AS pli_missing_quantity,
        count(distinct pli.package_line_item_id) as packages_count,
        
        from {{ ref('stg_package_line_items') }} as pli
        where pli.fulfillment != 'FAILED'
        --where line_item_id = 1215269
        group by 1
      ),

invoice_details as(
    select ii.line_item_id,
           sum(case when invoice_header_type = 'invoice' and invoice_item_status = 'APPROVED'  and i.generation_type = 'AUTO' then ii.price_without_tax else 0 end) as auto_gross_revenue,
           sum(case when invoice_header_type = 'credit note' and invoice_item_status = 'APPROVED' and i.generation_type = 'AUTO' then ii.price_without_tax else 0 end) as auto_credit_note,
           sum(case when i.invoice_header_type = 'invoice' then ii.quantity * li.unit_landed_cost else 0 end ) as total_cost,
           sum(case when invoice_header_type = 'invoice' and invoice_item_status = 'APPROVED' then ii.price_without_tax else 0 end) as gross_revenue,
           sum(case when invoice_header_type = 'credit note' and invoice_item_status = 'APPROVED' then ii.price_without_tax else 0 end) as credit_note,
           sum(delivery_charge_amount) as delivery_charge_amount,

    from {{ref("stg_invoice_items")}} ii
    left join {{ref('stg_invoices')}} i on ii.invoice_header_id = i.invoice_header_id
    left join {{ref ('stg_line_items')}} li on ii.line_item_id = li.line_item_id
    group by 1
),

package_line_items as (
    SELECT line_item_id,
             sum(quantity) as quantity,
             sum(fulfilled_quantity) as fulfilled_quantity,
             sum(damaged_quantity) as damaged_quantity,

    FROM {{ ref('stg_package_line_items') }}
    GROUP BY 1
)

SELECT

COALESCE(PackageLineItems.packed_quantity,0) as packed_quantity,
COALESCE(PackageLineItems.pli_fulfilled_quantity,0) as pli_fulfilled_quantity,
COALESCE(PackageLineItems.pli_missing_quantity,0) as pli_missing_quantity,
COALESCE(PackageLineItems.pli_fulfilled_quantity,0) * li.raw_unit_fob_price as received_fob,
(COALESCE(PackageLineItems.packed_quantity,0) - COALESCE(PackageLineItems.pli_missing_quantity,0)) as supplied_quantity,
 PackageLineItems.packages_count,

li.* EXCEPT(persona,order_type,delivery_date, departure_date,quantity,invoice_id,product_subcategory, product_category, li_record_type_details,li_record_type, invoice_number),


case 
    when li.persona = 'Reseller' and customer.account_type = 'External' then 'External Reseller'
    when li.persona = 'Reseller' and customer.account_type = 'Internal' then 'Internal Reseller'
    when li.persona = 'Customer' and customer.account_type = 'External' then 'External Customer'
    when li.persona = 'Customer' and customer.account_type = 'Internal' then 'Internal Customer'
    else li.persona
    end as persona,

case 
    when li.li_record_type_details != 'To Be Scoped' then li.li_record_type_details
    when li.li_record_type_details = 'To Be Scoped' and customer.debtor_number in ('132008','scriyadh')  then 'Cash and Carry Purchase Order'  --'BlooMax Flowers - Al khubar'. --Shop Customer Riyadh then li.li_record_type 
    else 'To Be Scoped'
end as li_record_type_details,

case 
    when li.li_record_type = 'To Be Scoped' and customer.debtor_number in ('132008','scriyadh')  then 'Purchase Order'  --'BlooMax Flowers - Al khubar'. --Shop Customer Riyadh then li.li_record_type 
    when li.li_record_type != 'To Be Scoped' then li.li_record_type
    else 'To Be Scoped'
end as li_record_type,




case when li.line_item_id is not null then li.total_price_without_tax else 0 end as potential_revenue,


i.signed_at,






li.quantity as ordered_quantity,

li.invoice_id as invoice_header_id,


case when li.order_type = 'OFFLINE' and orr.standing_order_id is not null then 'STANDING' else li.order_type end as order_type,
case when li.delivery_date is null  then date(li.created_at) else li.delivery_date end as delivery_date,
case when li.departure_date is null then date(li.created_at) else li.departure_date end as departure_date,



case when li.li_record_type_details in ('Reseller Purchase Order For Inventory') and li.location = 'loc' and pi.incidents_count is  null then 1 else 0 end as Received_not_scanned,

--actions
    returned_by.name as returned_by,
    dispatched_by.name as dispatched_by,
    created_by.name as created_by,
    split_by.name as split_by,
    order_requested_by.name as order_requested_by,




-- --funnel touchpoints 
    case when li.received_quantity > 0 then 1 else 0 end as order_received,
    case when li.fulfilled_quantity > 0 then 1 else 0 end as order_fulfilled,
    case when li.location = 'pod' then 1 else 0 end as order_pod_moved,



    case when li.state = 'DELIVERED' then 1 else 0 end as order_delivered,
    case when li.invoice_id is not null then 1 else 0 end as invoice_created,
    case when li.invoice_id is not null and i.invoice_header_printed_at is not null then 1 else 0 end as invoice_printed,


    case when li.location = 'loc' then 1 else 0 end as order_loc_moved, --order_warehoused
    -- case when li.picked_quantity > 0 then 1 else 0 end as order_picked,





--date
    date.dim_date,
    


--customer
    user.name as user,
    case when li.reseller_id is not null then  'Reseller' else customer.name  end as customer,
    customer.country,
    customer.financial_administration,
    customer.account_manager,
    customer.debtor_number,
    customer.customer_type,
    customer.account_type,
    customer.payment_term,
    customer.allow_due_invoices,
    customer.phone_number as customer_phone_number,
    customer.email as customer_email,

    customer.user_category as customer_category,
    concat( customer.financial_administration," - ", customer.Warehouse," - ", customer.account_type," - ", customer.customer_type," - ", customer.user_category," - ", customer.debtor_number  ) as customer_details,
    customer.city,
    case 
        when customer.payment_term = 'Without invoicing' then 'Without invoicing'
        when customer.payment_term = 'Cash on Delivery' then 'Cash on Delivery'
        else 'Invoicing'
        end as payment_term_type,

    case when customer.debtor_number in ('WANDE','95110') then 'Internal Invoicing' else 'Normal Invoicing' end as internal_invoicing,
        -- li.dispatched_at,
    case when li.received_quantity > 0 then 'Received' else 'Not Received' end as ops_status1,
    case when li.state in ('PENDING','CANCELED') then 'Not Fulfilled' else 'Fulfilled' end as ops_status2,
    case when li.location = 'pod' then 'Prepared' else 'Not Prepared' end as ops_status3,
    case when li.dispatched_at is not null then 'Dispatched' else 'Not Dispatched' end as ops_status4,
    case when li.state = 'DELIVERED' then 'Signed' else 'Not Signed' end as ops_status5,


    concat( "https://erp.floranow.com/line_items/", li.line_item_id) as line_item_link,

    

-- plis.supplier_name as parent_supplier,
    case when li.parent_line_item_id is not null then plis.supplier_name else lis.supplier_name end as Supplier,
    case when li.parent_line_item_id is not null then plis.supplier_region else lis.supplier_region end as supplier_region, --Origin


   -- sh.Supplier as shipment_Supplier,
    lis.supplier_name as raw_supplier,
    lis.supplier_region as raw_supplier_region,

--order 
    pli.order_type as parent_order_type,

    case 
        when li.li_record_type_details in ('Customer Sale Order From Fly-stock Inventory','Customer Sale Order From Direct Supplier') then 'Shipment Order To POD'  -- From Shipment External
        when li.li_record_type_details in ('Customer Sale Order From In-stock Inventory') then 'Express Order To POD'     --Inventory Orders (Stock-out)     -- From Inventory (stock out)
        when li.li_record_type_details in ('Reseller Purchase Order For Inventory') then 'Restocking Orders To LOC' -- --Reselling Orders (Stock-in) PO Orders (in) To Inventory Replenishment, Restocking
        when li.li_record_type_details in ('Customer Bulk Sale Order') then 'Bulk Orders'
        else 'To Be Scoped'
        end as fulfillment_mode,




                  

 
--order requist
    orr.status as order_request_status,
    concat( "https://erp.floranow.com/order_requests/", li.order_request_id) as order_request_link,
    case when li.order_request_id is not null then 'Order Request ID' else null end as order_request_cheack,
    case when li.replaced_quantity is not null then 'Replaced Qty.' else null end as replaced_quantity_cheack,


    --orr.quantity as requested_quantity,
    case when orr.quantity is not null then orr.quantity else li.quantity end as requested_quantity,


--order_payloads
    opl.offer_id as order_offer_id,
    opl.status as order_payloads_status,


--shipments
    sh.shipments_status, 
    sh.Shipment,
    --sh.shipment_id,
    concat( "https://erp.floranow.com/shipments/", sh.shipment_id) as shipment_link,
    concat( "https://erp.floranow.com/master_shipments/", msh.master_shipment_id) as master_shipment_link,
    msh.master_shipments_status,
    msh.master_shipment,

w.warehouse_name as warehouse,
w.warehouse_id,




COALESCE(pi.incident_quantity, 0) as incident_quantity,
COALESCE(pi.incident_quantity_without_extra, 0) as incident_quantity_without_extra,
COALESCE(pi.extra_quantity, 0) as extra_quantity,
COALESCE(pi.incident_quantity_inventory_dmaged, 0) as incident_quantity_inventory_dmaged,


COALESCE(pi.incidents_count, 0) as incidents_count,
COALESCE(pi.incidents_count_without_extra, 0) as incidents_count_without_extra,
COALESCE(pi.extra_count, 0) as extra_count,
COALESCE(pi.incidents_count_inventory_dmaged, 0) as incidents_count_inventory_dmaged,
COALESCE(pi.incidents_count_without_extra_without_inventory_dmaged, 0) as incidents_count_without_extra_without_inventory_dmaged,



COALESCE(pi.incident_cost, 0) as incident_cost,
COALESCE(pi.incident_cost_without_extra, 0) as incident_cost_without_extra,
COALESCE(pi.extra_cost, 0) as extra_cost,
COALESCE(pi.incident_cost_inventory_dmaged, 0) as incident_cost_inventory_dmaged,





COALESCE(pi.inventory_missing_quantity, 0) as inventory_missing_quantity,

COALESCE(pi.incident_quantity_packing_stage, 0) as incident_quantity_packing_stage,
COALESCE(pi.incident_quantity_receiving_stage, 0) as incident_quantity_receiving_stage,
COALESCE(pi.incident_quantity_inventory_stage, 0) as incident_quantity_inventory_stage,
COALESCE(pi.incident_quantity_delivery_stage, 0) as incident_quantity_delivery_stage,
COALESCE(pi.incident_quantity_after_return_stage, 0) as incident_quantity_after_return_stage,
COALESCE(pi.incident_quantity_before_supply_stage, 0) as incident_quantity_before_supply_stage,




COALESCE(pi.incident_quantity_extra_packing, 0) as incident_quantity_extra_packing,
COALESCE(pi.incident_quantity_extra_receiving, 0) as incident_quantity_extra_receiving,
COALESCE(pi.incident_quantity_extra_inventory, 0) as incident_quantity_extra_inventory,


COALESCE(pi.incident_orders_packing_stage, null) as incident_orders_packing_stage,
COALESCE(pi.incident_orders_receiving_stage, null) as incident_orders_receiving_stage,
COALESCE(pi.incident_orders_inventory_stage, null) as incident_orders_inventory_stage,

COALESCE(pi.incident_orders_delivery_stage, null) as incident_orders_delivery_stage,
COALESCE(pi.incident_orders_after_return_stage, null) as incident_orders_after_return_stage,



pod.source_type,
pod.pod_status,
pod.route_name,

case 
    when pod.route_name in ('Ajman', 'Sharjah', 'Northern Emirates', 'Ras Al Khaimah', 'Umm Al Quwain')  then 'Northern Emirates'
    when pod.route_name in ('Al Ain' ,'Al Ain 1') then 'Al Ain City'
    else pod.route_name end as vehicle_destination,



--pod.dispatched_by,



case 
    when date_diff(date(li.delivery_date)  ,current_date(), month) > 1 then 'Wrong date' 
    when li.delivery_date > current_date() then "Future" 
    when li.delivery_date = current_date() then "Today" 
    when li.delivery_date < current_date() then "Past" 
    else "cheak" end as select_delivery_date,





win.name as delivery_window,






case 
when customer.debtor_number  in ('SHPRUH','LNDHAI','ASTHAI','LNDJOU','ASTJOU','LNDHAF','ASTHAF','LNDQAS','ASTQAS','lndmed','astmed','EVEDMM', 'EVEHAF','EVEHAI','EVEJED','EVEJOU','EVEMED','EVEQAS','EVERUH','EVETUU','LNDDMM','ASTJED','LNDJED','FNQSIM','ASTRUH','LNDRUH','FSTUU') then 'BMX Reseller'
when customer.debtor_number  in ('130220','130009','130257','130188','132009','132008','134151','134150','820762') then 'BMX Original'
else null end as ksa_resellers,


  case 
        when li.invoice_id is not null and i.invoice_header_printed_at is not null then 'Invoice Printed' 
        when li.invoice_id is not null and i.invoice_header_printed_at is null then 'Invoice Created, Not Printed'
        else 'No Invoice ID' 
    end as invoice_status,


case when li.line_item_id is not null then 'Line Item ID' else null end as line_item_id_check,
case when li.order_id is not null then 'Order ID' else null end as order_id_check,
case when li.order_number is not null then 'Order Number ID' else null end as order_number_check,

case when li.root_shipment_id is not null then 'Root Shipment ID' else null end as root_shipment_id_check,



case when li.shipment_id is not null then 'Shipment ID' else null end as shipment_id_check,
case when li.invoice_id is not null then 'Invoice ID' else null end as invoice_id_check,

case 
when li.invoice_id is null then null 
when li.invoice_id is not null and i.invoice_header_printed_at is null then null 
else 'Invoice Number' end as invoice_number_check,


case when li.parent_line_item_id is not null then 'Parent ID' else null end as parent_id_check,

case when ppli.line_item_id is not null then 'Parent Parent ID' else null end as parent_parent_id_check,



case when li.source_line_item_id is not null then 'Source ID' else null end as source_id_check,
case when p.line_item_id is not null then 'Product ID' else null end as product_id_check,
case when li.offer_id is not null then 'Offer ID' else null end as offer_id_check,

case when li.reseller_id is not null then 'Reseller ID' else null end as reseller_id_check,
case when li.customer_id is not null then 'Customer ID' else null end as customer_id_check,
case when li.supplier_id is not null then 'Supplier ID' else null end as supplier_id_check,

case when li.reseller_id = li.customer_id then 'RC ID' else null end as reseller_customer_id_check,


case when li.customer_master_id is not null then 'Master ID' else null end as customer_master_id_check,
case when li.proof_of_delivery_id is not null then 'POD ID' else null end as proof_of_delivery_id_check,



p.product_id,


concat( "https://erp.floranow.com/products/", p.product_id) as product_link,
concat( "https://erp.floranow.com/line_items/", li.parent_line_item_id) as parent_line_item_link,

case 
when customer.debtor_number in ('130009','ASTHAI','EVEHAI','LNDHAI') then 'Hail'
WHEN customer.debtor_number in ('130220', 'ASTJOU', 'LNDJOU','EVEJOU') then 'Jouf'
WHEN customer.debtor_number in ('132009','ASTHAF','LNDHAF','EVEHAF') then 'Hafer'
WHEN customer.debtor_number in ('130257','LNDQAS','EVEQAS','FNQIM') then 'Qaseem'
else null end as samer_warehouses,




fs.feed_source_name,
fs.feed_type,
fs.supplier_name as feed_source_supplier,
reseller.name as Reseller,
reseller.reseller_label,
master.name as Master,

ii.quantity as inv_quantity,
ii.quantity * ii.unit_price as inv_total_price_without_tax,

ad.status as additional_status, 
ad.creation_stage as additional_creation_stage,
ad.additional_items_report_id,
concat( "https://erp.floranow.com/additional_items_reports/", ad.additional_items_report_id) as additional_item_link,
case when ad.additional_items_report_id is not null then 'Additional ID' else null end as additional_id_check,

concat( "https://erp.floranow.com/line_items/", li.source_line_item_id) as source_line_item_link,




-- `filled_product_categories`
    -- This model aims to address the null values in the `product_category` column of the `line_items` table.
    -- When a null value is encountered in `line_items`, it fetches the corresponding value from the `invoice_items` table (if available).
    -- This ensures a more complete dataset for analysis and reporting.
    -- It's important to note that `product_category` column of the `line_items` are dbt calculated metrics derived from the URL structure and not directly fetched from the database.
    case 
    when li.product_category is not null and ii.inv_product_category is not null then INITCAP(ii.inv_product_category) 
    when li.product_category is null then INITCAP(ii.inv_product_category) 
    when li.product_category is not null and ii.inv_product_category is null then  INITCAP(li.product_category) 
    else 'Ask Data Team' 
    end as product_category,


case 
when li.product_name like '%Lily Ot%' THEN 'Lily Or' 
when li.product_name like '%Lily Or%' THEN 'Lily Or' 
when li.product_name like '%Lily La%' THEN 'Lily La' 
when li.product_name like '%Li La%'  THEN 'Lily La' 
else INITCAP(li.product_subcategory) end as product_subcategory,

li.product_subcategory2 as sub_group,









i.invoice_header_status,
i.invoice_header_printed_at,
i.invoice_number,

st.stock_name as Stock,

case 
when st.stock_model in ('Reselling') then case when lis.supplier_name = 'ASTRA Farms' then 'Commission Based' else 'Reselling'
end else st.stock_model end as stock_model,

case 
when st.stock_model_details in ('Reselling') then case when lis.supplier_name = 'ASTRA Farms' then 'Commission Based - Astra Express' else 'Reselling' end
when st.stock_model_details in ('Reselling Event') then case when lis.supplier_name = 'ASTRA Farms' then 'Commission Based - Astra Express' else 'Reselling Event'  end
else st.stock_model_details end as stock_model_details,



case when li.order_source = 'Direct Supplier' then 1 else 0 end as direct_line_order_count,
case when li.order_source = 'Express Inventory' then 1 else 0 end as stock_line_order_count,

case when li.order_source = 'Direct Supplier' then li.order_id else null end as direct_order_ids,
case when li.order_source = 'Express Inventory' then li.order_id else null end as stock_order_ids,


case when li.fulfillment_status = 'Fulfilled' then 1 else 0 end as fulfilled_items,
case when li.dispatched_status = 'Dispatched' then 1 else 0 end as dispatched_items,
case when li.signed_status = 'Signed' then 1 else 0 end as signed_items,


concat(st.stock_id, " - ", st.stock_name, " - ", reseller.name  ) as full_stock_name,


pp.product_id as parent_product_id,



CASE 
  WHEN li.ordering_stock_type IS NOT NULL AND pp.product_id = SAFE_CAST(li.offer_id AS INT64) THEN 'ordering from reselling stocks product - GOOD'
  WHEN li.ordering_stock_type IS NOT NULL AND pp.product_id != SAFE_CAST(li.offer_id AS INT64) THEN 'ordering from reselling stocks product - To Be Scoped'
  WHEN li.ordering_stock_type IS NULL THEN 'ordering from external supplier offer'
  ELSE 'To Be Scoped'
END AS ordering_source_details,

sh.master_shipment_id,


li.supplier_product_name as supplier_product,
li.colour as product_color,

case when li.parent_line_item_id is not null then pli.raw_unit_fob_price else li.raw_unit_fob_price end as unit_fob_price,
case when li.parent_line_item_id is not null then pli.raw_fob_currency else li.raw_fob_currency end as fob_currency,


fmo.production_date_array,

origin.warehouse_name as source_warehouse,
destination.warehouse_name as destination_warehouse,

auto_gross_revenue,
auto_credit_note,
total_cost,
gross_revenue,
credit_note,
ind.delivery_charge_amount,

li.invoice_number as li_invoice_number,

case
    when li.order_source in ('Express Inventory') then 'Reselling'
    when li.order_source in ('Direct Supplier')  then 'Pre-Selling'
end as selling_stage,

case when li.order_type not in ('ADDITIONAL', 'EXTRA') then li.quantity - li.splitted_quantity - COALESCE(pi.incident_quantity_before_supply_stage, 0) end AS total_quantity,
pg.damaged_quantity AS damaged_packing_quantity,
pg.fulfilled_quantity AS sh_received_quantity,
case when li.order_type not in ('ADDITIONAL', 'EXTRA') then COALESCE(case when orr.quantity is not null then orr.quantity else li.quantity end, 0) end AS sh_requested_quantity,
CASE WHEN pg.quantity > 0 THEN COALESCE(pg.quantity, 0) - COALESCE(pi.missing_packing_quantity, 0) END AS sh_packed_quantity,

pi.missing_packing_quantity,
pi.sh_incident_quantity_receiving_stage,
pi.missing_quantity_receiving_stage,
damaged_quantity_receiving_stage,
pi.extra_quantity_receiving_stage AS extra_quantity_receiving_stage,
case when ad.creation_stage = 'PACKING' then ad.quantity END AS packing_additional_quantity,
case when ad.creation_stage = 'INVENTORY' then ad.quantity END AS inventory_additional_quantity,
case when li.order_type = 'EXTRA' and li.creation_stage = 'PACKING' then ordered_quantity END as extra_packing_quantity,

case when li.shipment_id is not null and li.warehoused_quantity > 0 then li.warehoused_quantity else 0 end as shipment_quantity,

-- case when cli.ordering_stock_type = 'INVENTORY' and customer.customer_type = 'retail' then cli.quantity else 0 end as retail_picked_qty,

-- case when cli.ordering_stock_type = 'INVENTORY' and customer.customer_type = 'reseller' then cli.quantity else 0 end as reseller_moved_qty,

incident_quantity_in_warehouse,

reseller_parent.name as parent_reseller,

warehousing_datetime,
pod_ready_datetime,

from {{ref('stg_line_items')}} as li
left join {{ ref('stg_products') }} as p on p.line_item_id = li.line_item_id 
left join {{ref('stg_order_requests')}} as orr on li.order_request_id = orr.id
left join {{ref('stg_order_payloads')}} as opl on li.order_payload_id = opl.order_payload_id
left join {{ref('stg_invoice_items')}} as ii on ii.line_item_id=li.line_item_id and ii.invoice_item_type = 'invoice'
-- left join {{ref('stg_invoice_items')}} as ii2 on ii2.line_item_id=li.line_item_id and ii2.invoice_item_type = 'credit note'
left join {{ref('stg_invoices')}} as i on li.invoice_id = i.invoice_header_id
left join {{ref('base_users')}} as master on master.id = li.customer_master_id
left join {{ref('base_users')}} as customer on customer.id = li.customer_id
left join {{ref('base_users')}} as reseller on reseller.id = li.reseller_id
left join {{ref('base_users')}} as user on user.id = li.user_id
left join {{ref('base_users')}} as dispatched_by on dispatched_by.id = li.dispatched_by_id
left join {{ref('base_users')}} as returned_by on returned_by.id = li.returned_by_id
left join {{ref('base_users')}} as created_by on created_by.id = li.created_by_id
left join {{ref('base_users')}} as split_by on split_by.id = li.split_by_id
left join {{ref('base_users')}} as order_requested_by on order_requested_by.id = orr.created_by_id

left join {{ref('base_suppliers')}} as lis on lis.supplier_id = li.supplier_id
left join {{ ref('stg_products') }} as pp on pp.line_item_id = li.parent_line_item_id 
left join {{ref('stg_line_items')}} as pli on pli.line_item_id = li.parent_line_item_id

left join {{ref('base_users')}} as reseller_parent on reseller_parent.id = pli.reseller_id

left join {{ref('stg_line_items')}} as ppli on ppli.line_item_id = pli.parent_line_item_id
left join {{ref('base_suppliers')}} as plis on plis.supplier_id = pli.supplier_id
left join {{ ref('dim_proof_of_deliveries') }} as pod on li.proof_of_delivery_id = pod.proof_of_delivery_id
left join {{ref('stg_shipments')}} as sh on li.shipment_id = sh.shipment_id
left join  {{ref('stg_master_shipments')}} as msh on sh.master_shipment_id = msh.master_shipment_id
left join {{ref('base_stocks')}} as st on p.stock_id = st.stock_id and p.reseller_id = st.reseller_id
left join {{ref('stg_feed_sources')}} as fs on fs.feed_source_id = li.feed_source_id
left join {{ref('base_warehouses')}} as w on w.warehouse_id = customer.warehouse_id
left join {{ref('base_warehouses')}} as origin on origin.warehouse_id = li.origin_warehouse_id
left join {{ref('base_warehouses')}} as destination on destination.warehouse_id = li.destination_warehouse_id
left join {{ref('stg_additional_items_reports')}}  as ad on ad.line_item_id=li.line_item_id
left join {{ref('dim_date')}}  as date on date.dim_date = date(li.created_at)
left join {{ref('stg_delivery_windows')}}  as win on  CAST(li.delivery_window_id AS INT64) = win.id
left join productIncidents as pi on pi.line_item_id = li.line_item_id
left join {{ref('int_fm_orders')}}  as fmo on  fmo.buyer_order_number = li.number

-- left join {{ref ("stg_line_items")}} cli on cli.parent_line_item_id = li.line_item_id

left join PackageLineItems on li.line_item_id = PackageLineItems.line_item_id
left join invoice_details ind on ind.line_item_id = li.line_item_id
LEFT JOIN package_line_items pg on li.line_item_id = pg.line_item_id 
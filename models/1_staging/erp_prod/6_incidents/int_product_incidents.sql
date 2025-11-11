
with

source as ( 
    with stock_data as (
    select product_incident_id, 
           st.stock_label,

    from {{ ref('stg_product_incidents') }} as pi
    left join {{ ref('int_line_items') }} as li on pi.line_item_id = li.line_item_id
    left join {{ ref('int_products') }} as p on p.line_item_id = coalesce(li.parent_line_item_id, li.line_item_id)
         join {{ ref('base_stocks') }} as st on st.stock_id = p.stock_id and st.reseller_id = p.reseller_id
    )    
select     

        pi.* EXCEPT(quantity),


        pi.quantity as incident_quantity,
        case when incident_type !='EXTRA'  then pi.quantity else 0 end as incident_quantity_without_extra,
        case when incident_type ='EXTRA'  then pi.quantity else 0 end as extra_quantity,
        case when master_report_filter = 'inventory_dmaged' then pi.quantity else 0 end as incident_quantity_inventory_dmaged,


        pi.quantity * li.unit_landed_cost as incident_cost,  -- damage, spoilage
        case when incident_type !='EXTRA'  then pi.quantity * li.unit_landed_cost else 0 end as incident_cost_without_extra,
        case when incident_type ='EXTRA'  then pi.quantity * li.unit_landed_cost else 0 end as extra_cost,
        case when master_report_filter = 'inventory_dmaged' then pi.quantity * li.unit_landed_cost else 0 end as incident_cost_inventory_dmaged,

        case when pi.product_incident_id is not null  then 1 else 0 end as incidents_count,
        case when incident_type !='EXTRA'  then 1 else 0 end as incidents_count_without_extra,
        case when incident_type ='EXTRA'  then 1 else 0 end as extra_count,
        case when master_report_filter = 'inventory_dmaged' then 1 else 0 end as incidents_count_inventory_dmaged,




        li.customer,
        li.Supplier,
        li.supplier_region as Origin,
        li.ordered_quantity,
        date(li.created_at) as order_date,
        date(li.delivery_date) as delivery_date,
        date(li.departure_date) as departure_date,
        li.stem_length,
        li.state,
        li.fulfillment_mode,

        li.li_record_type_details,
        li.li_record_type,
        li.order_source,

        li.Reseller,

        li.product_category,
        li.product_subcategory,
        li.product_name as Product,
        li.order_type,
        li.line_item_link,
        li.master_shipment,
        li.Shipment,
        li.unit_fob_price,
        li.unit_landed_cost,
        li.order_number,
        li.production_date_array,


        pi.quantity * li.unit_fob_price as fob_value,



        reported_by.name as reported_by,



        CONCAT(COALESCE(pi.incident_type, ''), '-', COALESCE(pi.reason, '')) as type_reason,








        pi.quantity * li.unit_fob_price as incident_fob_value,

        li.currency,
        li.fob_currency,
        li.customer_id,
        li.ordering_stock_type,
        li.feed_source_name,
        li.unit_price,
        li.selling_stage,


     
        case 
            when accountable_type = 'User' then accountable_User.name
            when accountable_type = 'Supplier' then accountable_Supplier.name
            else 'cheak'
            end as Accountable,


            w2.warehouse_name as warehouse,
            w2.warehouse_country,
            w2.box_label,
         --   w2.financial_administration,

/*
case 
when pi.stage != 'INVENTORY' then null
when pi.incident_type = 'DAMAGED'  then 'inventory_dmaged'
when pi.incident_type != 'DAMAGED'  then 'inventory_incidents'
--when pi.incidentable_type in ('ProductLocation','Product') and 
--when pi.incidentable_type = 'LineItem' and pi.incident_type not in ('DAMAGED') then 'inventory_incidents'
else null  
end as report_filter_inventory,

case when pi.stage in ('PACKING', 'RECEIVING') then 'supplier_incidents' else null end as  report_filter_supplier,
*/


concat( "https://erp.floranow.com/product_incidents/", pi.product_incident_id) as incidents_link,

customer.financial_administration,
customer.debtor_number,

ii.invoice_item_id,

case 
when pi.line_item_id is not null and ii.invoice_item_id is not null then 'with Li and Inv' 
when pi.line_item_id is not null then 'with Li' 
when ii.invoice_item_id is not null then 'with Inv' 
else 'check' 
end as pi_record_type,





p.Stock,
p.stock_model_details,
p.stock_model,
p.full_stock_name,
p.modified_stock_model,
sd.stock_label,

concat('NCR-', FORMAT_TIMESTAMP('%y%m%d', li.departure_date), '-', li.shipment_id) as NCR,


case 
when li.customer_id in (1289,1470,2816,11123) then 'Cash and Carry Reseller' 
else 'Normal Reseller' 
end as reseller_type,

--Retail Bloomax Flowers Hail  - 130009
--BlooMax Flowers - Al khubar - 132008
--Bloomax - Hafer Al baten - 132009
--BX Shop Express Riyadh - 11123

CASE 
    WHEN li.order_type ='MOVEMENT' Then 'Indirect Damage'   
    WHEN li.order_type ='OFFLINE' AND li.feed_source_name IN ('DMM Project X','JED Project X','Project X',
    'Grandiose TBF','TBF RUH','FN WEDDINGS AND EVENTS RES','RUH Wedding and Events','GRD Stock','Jumerah',
    'MAF Stock','Spinneys','Spinneys Stock Order','The buqat factory')
    Then 'Indirect Damage'else 'Direct Damage' END AS damage_type,

customer.account_manager,
customer.user_category,
customer.reseller_label,

case when pi.stage = 'INVENTORY' and pi.incident_type = 'DAMAGED' and after_sold = false and sd.stock_label is not null then pi.quantity * li.unit_landed_cost else 0 end as inventory_damaged_cost,

current_timestamp() as insertion_timestamp,

0 as new_data,

from {{ ref('stg_product_incidents')}} as pi
left join {{ref('int_line_items')}} as li on pi.line_item_id = li.line_item_id

left join {{ref('int_products')}} as p on p.line_item_id = li.line_item_id 

left join {{ref('base_users')}} as reported_by on reported_by.id = pi.reported_by_id

left join {{ref('base_users')}} as customer on customer.id = li.customer_id


left join {{ref('base_users')}} as accountable_User on accountable_User.id = pi.accountable_id  and pi.accountable_type = 'User'
left join {{ref('base_users')}} as accountable_Supplier on accountable_Supplier.id = pi.accountable_id  and pi.accountable_type = 'Supplier'

left join {{ref('base_warehouses')}} as w2 on w2.warehouse_id = customer.warehouse_id



left join {{ref('stg_invoice_items')}} as ii on ii.invoice_item_id=pi.credit_note_item_id 

left join stock_data sd on pi.product_incident_id = sd.product_incident_id

    )

select * from source

union all

SELECT
        incident_id as product_incident_id,

        NULL AS line_item_id,
        NULL AS incidentable_id, 
        NULL AS credit_note_item_id, 
        NULL AS accountable_id,
        NULL AS location_id, 
        NULL AS inventory_cycle_check_id, 

        incident_date as incident_at,
        NULL AS deleted_at,
        NULL AS updated_at,

        'DAMAGED' AS incident_type,
        NULL AS incidentable_type,
        NULL AS accountable_type,
        'INVENTORY' AS stage, 

        NULL AS reported_by_id,

        NULL AS credited, 
        false AS after_sold, 

        NULL AS status, 
        NULL AS note,
        NULL AS reason,


        -- allocated_damage_quantity as quantity,               
        NULL AS valid_quantity,
        NULL AS accounted_quantity,


        'Incident' AS record_type,

        'Inventory Dmaged' AS incident_report,
        'inventory_dmaged' AS master_report_filter,
        current_timestamp() as insertion_timestamp,

        allocated_damage_quantity as incident_quantity,
        allocated_damage_quantity as incident_quantity_without_extra,
        NULL as extra_quantity,
        allocated_damage_quantity as incident_quantity_inventory_dmaged,


        allocated_damage_cost as incident_cost,  -- damage, spoilage
        allocated_damage_cost as incident_cost_without_extra,
        NULL as extra_cost,
        allocated_damage_cost as incident_cost_inventory_dmaged,

        case when incident_id is not null  then 1 else 0 end as incidents_count,
        case when incident_id is not null  then 1 else 0 end as incidents_count_without_extra,
        null as extra_count,
        case when incident_id is not null then 1 else 0 end as incidents_count_inventory_dmaged,




        null as customer,
        "ASTRA Farms" as Supplier,
        "Saudi Arabia" as Origin,
        null as ordered_quantity,
        CAST(null as date) as order_date,
        CAST(null as date) as delivery_date,
        CAST(null as date) as departure_date,
        stem_length,
        "FULFILLED" as state,
        "To Be Scoped" as fulfillment_mode,

        null as li_record_type_details,
        null as li_record_type,
        null as order_source,

        null as Reseller,

        product_category,
        product_subcategory,
        Product,
        null as order_type,
        null as line_item_link,
        null as master_shipment,
        null as Shipment,
        fob_unit_price as unit_fob_price,
        fob_unit_price as unit_landed_cost,
        null as order_number,
        null as production_date_array,


        null as fob_value,

        null as reported_by,
        null as type_reason,

        allocated_damage_quantity * fob_unit_price as incident_fob_value,

        null as currency,
        null as fob_currency,
        null as customer_id,
        null as ordering_stock_type,
        null as feed_source_name,
        fob_unit_price as unit_price,
        null as selling_stage,


     
       'User' as Accountable,


        warehouse_name as warehouse,
        null as warehouse_country,
        null as box_label,


concat( "https://erp.floranow.com/fm/product_incidents/", incident_id) as incidents_link,

"KSA" as financial_administration,
null as debtor_number,

null as invoice_item_id,

'check' as pi_record_type,

null as Stock,
null as stock_model_details,
null as stock_model,
null as full_stock_name,
null as modified_stock_model,
"Astra" as stock_label,

null as NCR,


null as reseller_type,


'Direct Damage' AS damage_type,

null as account_manager,
null as user_category,
null as reseller_label,

allocated_damage_cost as inventory_damaged_cost,

current_timestamp() as insertion_timestamp,

1 as new_data,

    FROM {{ ref('stg_astra_incidents')}}
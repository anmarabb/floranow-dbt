
with

source as ( 
        
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

        case when product_incident_id is not null  then 1 else null end as incidents_count,
        case when incident_type !='EXTRA'  then 1 else null end as incidents_count_without_extra,
        case when incident_type ='EXTRA'  then 1 else null end as extra_count,
        case when master_report_filter = 'inventory_dmaged' then 1 else null end as incidents_count_inventory_dmaged,




        li.order_type,
        li.customer,
        li.Supplier,
        li.ordered_quantity,
        li.created_at as order_date,
        li.delivery_date,
        li.state,

        reported_by.name as reported_by,






        pi.quantity * li.unit_fob_price as incident_fob_value,

        li.currency,
        li.fob_currency,



     
        case 
            when accountable_type = 'User' then accountable_User.name
            when accountable_type = 'Supplier' then accountable_Supplier.name
            else 'cheak'
            end as Accountable,


            w2.warehouse_name as warehouse,
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

ii.invoice_item_id,

case 
when pi.line_item_id is not null and ii.invoice_item_id is not null then 'with Li and Inv' 
when pi.line_item_id is not null then 'with Li' 
when ii.invoice_item_id is not null then 'with Inv' 
else 'check' 
end as pi_record_type,









        current_timestamp() as insertion_timestamp,

from {{ ref('stg_product_incidents')}} as pi
left join {{ref('int_line_items')}} as li on pi.line_item_id = li.line_item_id
left join {{ref('int_products')}} as p on p.line_item_id = li.line_item_id 

left join {{ref('base_users')}} as reported_by on reported_by.id = pi.reported_by_id

left join {{ref('base_users')}} as customer on customer.id = li.customer_id


left join {{ref('base_users')}} as accountable_User on accountable_User.id = pi.accountable_id  and pi.accountable_type = 'User'
left join {{ref('base_users')}} as accountable_Supplier on accountable_Supplier.id = pi.accountable_id  and pi.accountable_type = 'Supplier'

left join {{ref('base_warehouses')}} as w2 on w2.warehouse_id = customer.warehouse_id



left join {{ref('stg_invoice_items')}} as ii on ii.invoice_item_id=pi.credit_note_item_id 






    )

select * from source
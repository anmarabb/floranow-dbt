
with

source as ( 
        
select     

        pi.* EXCEPT(quantity),
        pi.quantity as incident_quantity,

        li.order_type,
        li.customer,
        li.Supplier,
        li.ordered_quantity,

        reported_by.name as reported_by,


        pi.quantity * li.unit_landed_cost as incident_value,  -- damage, spoilage
        pi.quantity * li.unit_fob_price as incident_fob_value,

        li.currency,
        li.fob_currency,



     
        case 
            when accountable_type = 'User' then accountable_User.name
            when accountable_type = 'Supplier' then accountable_Supplier.name
            else 'cheak'
            end as Accountable,


            w2.warehouse_name as Warehouse,
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

case 
when pi.stage in ('PACKING', 'RECEIVING') then 'supplier_incidents'
when pi.stage = 'DELIVERY' then 'DELIVERY'
when pi.stage = 'AFTER_RETURN' then 'AFTER_RETURN'
when pi.stage = 'INVENTORY' and pi.incident_type = 'DAMAGED'  then 'inventory_dmaged'
when pi.stage = 'INVENTORY' and pi.incident_type != 'DAMAGED'  then 'inventory_incidents'
else null  
end as master_report_filter,

concat( "https://erp.floranow.com/product_incidents/", pi.product_incident_id) as incidents_link,

customer.financial_administration,

        current_timestamp() as insertion_timestamp,

from {{ ref('stg_product_incidents')}} as pi
left join {{ref('int_line_items')}} as li on pi.line_item_id = li.line_item_id
left join {{ref('int_products')}} as p on p.line_item_id = li.line_item_id 

left join {{ref('base_users')}} as reported_by on reported_by.id = pi.reported_by_id

left join {{ref('base_users')}} as customer on customer.id = li.customer_id


left join {{ref('base_users')}} as accountable_User on accountable_User.id = pi.accountable_id  and pi.accountable_type = 'User'
left join {{ref('base_users')}} as accountable_Supplier on accountable_Supplier.id = pi.accountable_id  and pi.accountable_type = 'Supplier'

left join {{ref('base_warehouses')}} as w2 on w2.warehouse_id = customer.warehouse_id





where  pi.deleted_at is null
    )

select * from source
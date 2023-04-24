
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


     
        case 
            when accountable_type = 'User' then accountable_User.name
            when accountable_type = 'Supplier' then accountable_Supplier.name
            else 'cheak'
            end as Accountable,

        current_timestamp() as insertion_timestamp,

from {{ ref('stg_product_incidents')}} as pi
left join {{ref('int_line_items')}} as li on pi.line_item_id = li.line_item_id
left join {{ref('int_products')}} as p on p.line_item_id = li.line_item_id 

left join {{ref('base_users')}} as reported_by on reported_by.id = pi.reported_by_id


left join {{ref('base_users')}} as accountable_User on accountable_User.id = pi.accountable_id  and pi.accountable_type = 'User'
left join {{ref('base_users')}} as accountable_Supplier on accountable_Supplier.id = pi.accountable_id  and pi.accountable_type = 'Supplier'






where  pi.deleted_at is null
    )

select * from source
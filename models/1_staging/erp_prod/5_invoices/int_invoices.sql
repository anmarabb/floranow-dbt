with

source as ( 
        
select     

i.*,



concat(customer.debtor_number,i.items_collection_date) as drop_id, 



    printed_by.name as printed_by,

    customer.account_manager,
    customer.City,
    customer.user_category as client_category,
    customer.name as Customer,
    customer.Warehouse,





    current_timestamp() as insertion_timestamp, 

from {{ ref('stg_invoices')}} as i
left join {{ ref('base_users') }} as printed_by on printed_by.id = i.printed_by_id
left join {{ ref('base_users') }} as customer on customer.id = i.customer_id




    )

select * from source
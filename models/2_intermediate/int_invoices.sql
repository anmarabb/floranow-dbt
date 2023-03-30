with

source as ( 
        
select     

i.*,

    current_timestamp() as insertion_timestamp, 

from {{ ref('stg_invoices')}} as i
left join {{ ref('base_users') }} as customer on customer.id = i.customer_id




    )

select * from source
with

source as ( 
        
select     

i.*,

   
    printed_by.name as printed_by,




    current_timestamp() as insertion_timestamp, 

from {{ ref('stg_invoices')}} as i
left join {{ ref('base_users') }} as printed_by on printed_by.id = i.printed_by_id




    )

select * from source
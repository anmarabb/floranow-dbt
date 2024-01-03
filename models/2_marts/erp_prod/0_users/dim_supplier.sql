with

source as ( 
        
select     

supplier_name,
currency,
account_manager,
supplier_region,
country,



    current_timestamp() as insertion_timestamp 

from {{ ref('int_supplier')}} as s

)

select * from source

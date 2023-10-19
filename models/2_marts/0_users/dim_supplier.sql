with

source as ( 
        
select     

*,


    current_timestamp() as insertion_timestamp 

from {{ ref('int_supplier')}} as s

)

select * from source

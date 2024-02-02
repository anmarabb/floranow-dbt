with

source as ( 
        
select     
*,
    current_timestamp() as insertion_timestamp, 

from {{ ref('base_account_managers')}} as a

)

select * from source
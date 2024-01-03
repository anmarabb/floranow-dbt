with

source as ( 
        
select     
*,
    current_timestamp() as insertion_timestamp, 

from {{ ref('base_account_managers')}} as a
--left join {{ ref('budget') }} as b on b.account_manager = a.account_manager

)

select * from source
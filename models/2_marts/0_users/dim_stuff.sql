with

source as ( 
        
select     
    u.id as customer_id,
    u.name,
    u.debtor_number,
    u.account_type,
    u.customer_type,
    current_timestamp() as insertion_timestamp, 

from {{ ref('base_users')}} as u

where account_type = 'Internal'and customer_type != 'reseller')

select * from source
with

source as ( 
        
select     
    u.id as reseller_id,
    u.name as reseller_name,
    u.debtor_number as reseller_debtor_number,
    u.account_type,
    warehouse_id,
    account_manager,
    user_category,
    country,
    payment_term,
    financial_administration,
   -- warehouse_name,


    current_timestamp() as insertion_timestamp, 

from {{ref('base_users')}} as u

where customer_type = 'reseller')


select * from source
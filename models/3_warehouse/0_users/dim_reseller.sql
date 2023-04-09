with

source as ( 
        
select     
    u.id as reseller_id,
    u.name as reseller_name,
    u.debtor_number as reseller_debtor_number,
    u.account_type,
    u.customer_type,
    u.financial_administration,
    u.payment_term,
    u.country,
    u.user_category,
    u.account_manager,

    current_timestamp() as insertion_timestamp, 

from {{ ref('base_users')}} as u

where  u.customer_type = 'reseller')

select * from source
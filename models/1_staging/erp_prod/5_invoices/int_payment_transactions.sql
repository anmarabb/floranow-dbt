with

source as ( 
        
select     

pt.*,

customer.name as Customer,
customer.account_manager,
customer.debtor_number,
customer.company_name,
customer.city,
customer.user_category,


    current_timestamp() as insertion_timestamp, 

from {{ ref('stg_payment_transactions')}} as pt
left join {{ ref('base_users') }} as customer on customer.id = pt.user_id




    )

select * from source


--where payment_transaction_id = 966217

--950491
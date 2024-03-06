with a as (
--query for unreconciled payment transactions amount

with unreconciled_payment as (

    select

        user_id,
        case when cmi.date is not null then cmi.date else cmi.date end as master_date,
        --cmi.date as master_date,
        payment_received_at,
        
        Customer,
        debtor_number, 
        account_manager,        
        user_category as client_category,
        company_name,
        warehouse,
        financial_administration,
        payment_method,

        payment_transaction_number,
        credit_note_number,
        CAST(NULL AS STRING) as invoice_number,
        approval_code,




        abs(cmi.residual) as payment_amount,
        null as  reconciled_payment_amount,
        abs(cmi.residual) as unreconciled_payment_amount,

        CASE WHEN LOWER(Customer) LIKE '%bloomax%' THEN 'Bloomax Customers'  ELSE 'Include' END AS payment_filter,
--
        case when  date_diff(current_date() , date(cmi.date)  , MONTH) = 0 then COALESCE(abs(cmi.residual),0) else 0 end as mtd_paymnets,
        case when  date_diff(current_date() , date(cmi.date)  , MONTH) = 1 then COALESCE(abs(cmi.residual),0) else 0 end as m_1_paymnets,
        case when  date_diff(current_date() , date(cmi.date)  , MONTH) = 2 then COALESCE(abs(cmi.residual),0) else 0 end as m_2_paymnets,

    from {{ref('fct_move_items')}} as cmi 
    where cmi.entry_type='CREDIT'
    and  round(cmi.residual, 2) != 0 
    and cmi.documentable_id is not null
    )

 select

*

from unreconciled_payment
   
union all

select
user_id,
master_date,  --case when pt.payment_received_at is not null then pt.payment_received_at else cmi.date end
payment_received_at,


Customer,
debtor_number,
account_manager,
user_category,
company_name,
warehouse,
financial_administration,
 case 
 when payment_method is not null then payment_method 
 when payment_type is not null then UPPER(payment_type)
 else transaction_type end as payment_method,


payment_transaction_number,
credit_note_number,
invoice_number,
approval_code,


payment_amount,
payment_amount as reconciled_payment_amount,
null as unreconciled_payment_amount,

CASE WHEN LOWER(Customer) LIKE '%bloomax%' THEN 'Bloomax Customers'  ELSE 'Include' END AS payment_filter,

        case when  date_diff(current_date() , date(master_date)  , MONTH) = 0 then COALESCE(payment_amount,0) else 0 end as mtd_paymnets,
        case when  date_diff(current_date() , date(master_date)  , MONTH) = 1 then COALESCE(payment_amount,0) else 0 end as m_1_paymnets,
        case when  date_diff(current_date() , date(master_date)  , MONTH) = 2 then COALESCE(payment_amount,0) else 0 end as m_2_paymnets,

--current_timestamp() as insertion_timestamp,


from {{ref('int_payments')}} as py

)

select * from a




WITH registered_clients AS 
(
    SELECT 
        financial_administration_id,
        count(*)  as registered_clients , 


    FROM  {{ ref('base_users') }} as u
    where 
        account_type in ('External') 
        and deleted_accounts != 'Deleted' 
        and fake_temp = 'normal'
    GROUP BY
        u.financial_administration_id

)
 

select 
rc.registered_clients,


id,
name,
prefix,
created_at,
updated_at,
start_invoice_number,
current_invoice_number,
invoice_prefix,
credit_note_prefix,
payment_transaction_prefix,


current_timestamp() as ingestion_timestamp,


from {{ source('erp_prod', 'financial_administrations') }} as fn
left join registered_clients as rc on rc.financial_administration_id = fn.id
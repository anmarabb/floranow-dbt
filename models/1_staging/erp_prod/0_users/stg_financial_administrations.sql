


WITH registered_clients AS 
(
    SELECT 
        financial_administration_id,
        count(*)  as registered_clients , 


    FROM  {{ ref('base_users') }} as u
    where 
        account_type in ('External') 
        and user_validity_filter = 'normal'
    GROUP BY
        u.financial_administration_id

)
 

select 
rc.registered_clients,


id,


case when name = 'Saudi' then 'KSA' else name end as name,


prefix,
created_at,
updated_at,
start_invoice_number,
--current_invoice_number,
invoice_prefix,
credit_note_prefix,
payment_transaction_prefix,


current_timestamp() as ingestion_timestamp,


from {{ source(var('erp_source'), 'financial_administrations') }} as fn
left join registered_clients as rc on rc.financial_administration_id = fn.id
With source as (


select 

a.account_manager_type,
u.name as account_manager,
f.name as financial_administration,

from {{ source('erp_prod', 'account_managers') }} as a
left join {{ source('erp_prod', 'users') }} as u on a.user_id = u.id
left join {{ ref('stg_financial_administrations') }} as f on f.id = u.financial_administration_id



)
select 

*,

current_timestamp() as ingestion_timestamp,

 




from source as ii
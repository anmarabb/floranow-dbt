With source as (
 select * from {{ source('erp_prod', 'sections') }}
)
select 

id as section_id,
name as section_name,
warehouse_id,
checking_status,
warehouse_checking_status,

created_at,
updated_at,

from source 

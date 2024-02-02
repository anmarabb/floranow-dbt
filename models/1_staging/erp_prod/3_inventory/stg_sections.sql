With source as (
 select * from {{ source(var('erp_source'), 'sections') }}
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

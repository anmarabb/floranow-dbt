with 

prep_countryas as (select distinct country_iso_code  as code, country_name from {{ source(var('erp_source'), 'country') }}  ),
base_manageable_accounts_supplier as 

(
select
account_manager_id,
manageable_id,

from {{ source(var('erp_source'), 'manageable_accounts') }} 
where manageable_type = 'Supplier'
)



select 

s.id as supplier_id,
s.name as supplier_name,
s.business_type,
s.currency,
s.floranow_supplier_id,

case when s.packing_method = 1 then 'auto_email' when s.packing_method = 0 then 'manual' when s.packing_method is null then null else 'ceack' end as packing_method,
case when s.packing_receive_type = 0 then 'barcode' when s.packing_receive_type is null then null else 'ceack' end as packing_receive_type,

case when s.has_box_number is true then 'has_box_number' else null end as has_box_number,
s.auto_send,
s.is_local_purchase,


s.created_at,
s.updated_at,
s.deleted_at,
--email,
--phone_no,
--company_name,
--country,
--floricode_id,
--active,
--resource_id,

u2.name as account_manager ,
c.country_name as supplier_region, --Origin
s.country,


current_timestamp() as ingestion_timestamp,

from {{ source(var('erp_source'), 'suppliers') }} as s
left join prep_countryas as c on s.country = c.code
left join base_manageable_accounts_supplier as mas on mas.manageable_id = s.id 
left join {{ source(var('erp_source'), 'account_managers') }} as account_m on mas.account_manager_id = account_m.id
left join {{ source(var('erp_source'), 'users') }} as u2 on u2.id = account_m.user_id




With source as (
 select * from {{ source('erp_prod', 'warehouses') }}
)
select 

w.id as warehouse_id,
w.name as warehouse_name,

w.country,
w.region_name as warehouse_region,
w.reseller_id,

w.company_id,
w.landing_region_id,
w.status,
w.created_at,
w.updated_at,
w.deleted_at, 



current_timestamp() as ingestion_timestamp,
 




from source as w
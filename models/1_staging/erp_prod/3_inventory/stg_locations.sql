With source as (
 select * from {{ source(var('erp_source'), 'locations') }}
)
select 

id as location_id,
section_id,
warehouse_id,


label,

number,
section,
status,
barcode,

warehouse_checking_status,
section_checking_status,

created_at,
deleted_at,
updated_at,
--name,

current_timestamp() as ingestion_timestamp,
 



from source as loc


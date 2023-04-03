With source as (
 select * from {{ source('erp_prod', 'stocks') }}
)
select 

id as stock_id,
name as stock_name,
warehouse_id,
reseller_id,


case when stock_type = 0 then 'inventory' else 'flying' end as stock_type,
case when status = 0 then 'visible' else 'hidden' end as stock_status,

out_feed_source_id,

availability_type,

current_timestamp() as ingestion_timestamp,
 




from source as st
With source as (
 select * from {{ source('erp_prod', 'warehouses') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as w

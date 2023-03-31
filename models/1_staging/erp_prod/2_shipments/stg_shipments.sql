With source as (
 select * from {{ source('erp_prod', 'shipments') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as sh
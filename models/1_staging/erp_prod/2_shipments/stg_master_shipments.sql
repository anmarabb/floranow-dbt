With source as (
 select * from {{ source('erp_prod', 'master_shipments') }}
)
select 

*,

current_timestamp() as ingestion_timestamp, 




from source as msh
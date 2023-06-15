With source as (
 select * from {{ source('erp_prod', 'delivery_windows') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source 
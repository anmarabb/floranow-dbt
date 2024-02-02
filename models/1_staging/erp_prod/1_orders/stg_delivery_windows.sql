With source as (
 select * from {{ source(var('erp_source'), 'delivery_windows') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source 
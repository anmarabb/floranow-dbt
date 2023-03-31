With source as (
 select * from {{ source('erp_prod', 'packages') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as packages
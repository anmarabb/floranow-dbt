With source as (
 select * from {{ source('erp_prod', 'products') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as p
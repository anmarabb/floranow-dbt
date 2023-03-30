With source as (
 select * from {{ source('erp_prod', 'packing_lists') }}
)
select 
*,

current_timestamp() as ingestion_timestamp,
 




from source as packlist
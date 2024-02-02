With source as (
 select * from {{ source(var('erp_source'), 'companies') }}
)
select 
*,

current_timestamp() as ingestion_timestamp,




from source 
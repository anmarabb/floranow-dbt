With source as (
 select * from {{ source('erp_prod', 'additional_items_reports') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as ad
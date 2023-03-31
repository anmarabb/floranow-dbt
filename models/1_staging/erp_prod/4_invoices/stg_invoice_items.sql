With source as (
 select * from {{ source('erp_prod', 'invoice_items') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,

 




from source as ii

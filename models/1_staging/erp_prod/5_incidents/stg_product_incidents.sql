With source as (
 select * from {{ source('erp_prod', 'product_incidents') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,




from source as pi
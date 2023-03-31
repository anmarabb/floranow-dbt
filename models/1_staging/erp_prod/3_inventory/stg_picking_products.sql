With source as (
 select * from {{ source('erp_prod', 'picking_products') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as p
With source as (
 select * from {{ source('erp_prod', 'order_requests') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as orr
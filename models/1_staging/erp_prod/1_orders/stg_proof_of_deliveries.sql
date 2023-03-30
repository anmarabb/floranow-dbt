With source as (
 select * from {{ source('erp_prod', 'proof_of_deliveries') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as pod
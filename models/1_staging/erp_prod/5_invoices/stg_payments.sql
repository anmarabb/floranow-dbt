With source as (
 select * from {{ source('erp_prod', 'payments') }}
)
select 

*,
current_timestamp() as ingestion_timestamp

 
from source as i 
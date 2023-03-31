With source as (
 select * from {{ source('erp_prod', 'feed_sources') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,




from source as fs
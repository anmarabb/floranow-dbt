With source as (
 select * from {{ source('erp_prod', 'feed_source_regions') }}
)
select 
*,

current_timestamp() as ingestion_timestamp,
 



from source as fsr


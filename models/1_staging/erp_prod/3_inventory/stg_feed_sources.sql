With source as (
 select * from {{ source('erp_prod', 'feed_sources') }}
)
select 

id as feed_source_id,
name as feed_source_name,
supplier_id,
currency,
supplying_country_code,
reselling,
floranow_feed_id,
feed_type,
availability_type,


current_timestamp() as ingestion_timestamp,




from source as fs
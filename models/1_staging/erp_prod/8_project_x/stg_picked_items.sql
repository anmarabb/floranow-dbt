With source as (
 select * from {{ source(var('erp_source'), 'picked_items') }}  )
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source
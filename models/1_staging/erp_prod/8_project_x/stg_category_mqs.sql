With source as (
 select * from {{ source(var('erp_source'), 'item_movement_requests') }}  )
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source
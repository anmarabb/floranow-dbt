With source as (
 select * from {{ source(var('erp_source'), 'product_location_movements') }}  )
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source
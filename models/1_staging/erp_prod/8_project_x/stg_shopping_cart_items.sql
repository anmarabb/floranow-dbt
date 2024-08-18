With source as (
 select * from {{ source(var('erp_source'), 'shopping_cart_items') }}  )
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source
With source as (
 select * from {{ source(var('erp_source'), 'category_mqs') }}  )
select 
*,


current_timestamp() as ingestion_timestamp,
 



from sources
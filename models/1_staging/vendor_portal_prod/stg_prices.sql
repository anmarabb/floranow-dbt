With source as (
 select * from {{ source(var('vrp_source'), 'prices') }}
)
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source as loc
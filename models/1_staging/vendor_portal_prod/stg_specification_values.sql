With source as (
 select * from {{ source(var('vrp_source'), 'specification_values') }}
)
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source as loc
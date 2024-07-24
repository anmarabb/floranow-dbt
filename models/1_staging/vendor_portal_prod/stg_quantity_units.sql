With source as (
 select * from {{ source(var('vrp_source'), 'quantity_units') }}
)
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source as loc
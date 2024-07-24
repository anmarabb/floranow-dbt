With source as (
 select * from {{ source(var('vrp_source'), 'offer_templates') }}
)
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source as loc
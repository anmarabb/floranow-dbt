With source as (
 select * from {{ source(var('mkp_source'), 'spree_vendors') }}
)
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source as loc


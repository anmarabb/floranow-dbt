With source as (
 select * from {{ source(var('mkp_source'), 'spree_offering_windows') }}
 
)
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source as loc


With source as (
 select * from {{ source('marketplace_prod', 'spree_offering_windows') }}
 
)
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source as loc


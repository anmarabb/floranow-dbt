With source as (
 select * from {{ source('marketplace_prod', 'spree_regions') }}
)
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source as loc


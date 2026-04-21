With source as (
 select * from {{ source(var('marketplace_prod_master_rds'), 'spree_vendors') }}
)
select 
*,


current_timestamp() as ingestion_timestamp,
 



from source as loc


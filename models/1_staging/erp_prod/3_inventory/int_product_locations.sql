With source as (

    
 select 
 
*

from {{ ref('stg_product_locations') }} as pl
 
 
 
)
select 

*,

current_timestamp() as ingestion_timestamp,
 
 



from source


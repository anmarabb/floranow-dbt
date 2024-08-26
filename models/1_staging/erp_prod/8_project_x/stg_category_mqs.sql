With source as (
 select * from {{ source(var('erp_source'), 'category_mqs') }}  )

select INITCAP(main_group) as sub_category,
       sub_group,
       Product,
       product_color,
       MQS,


current_timestamp() as ingestion_timestamp,
 
from source
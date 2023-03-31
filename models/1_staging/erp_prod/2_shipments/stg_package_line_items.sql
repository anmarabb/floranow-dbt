With source as (
 select * from {{ source('erp_prod', 'package_line_items') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as packages_li
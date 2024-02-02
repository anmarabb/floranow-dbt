With source as (
 select * from {{ source(var('erp_source'), 'packing_box_items') }}
)
select 

*,

current_timestamp() as ingestion_timestamp,
 




from source as packbox
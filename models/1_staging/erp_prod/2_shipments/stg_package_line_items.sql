With source as (
 select * from {{ source(var('erp_source'), 'package_line_items') }}
)
select 


id as package_line_item_id,
package_id,
line_item_id,


created_at,
updated_at,
status,      --PENDING, WAREHOUSED, INSPECTED
fulfillment,  --UNACCOUNTED, FAILED, FAILED, PARTIAL
damaged_note,


quantity,
fulfilled_quantity,
damaged_quantity,
extra_quantity,
missing_quantity,
warehoused_quantity,
received_quantity,

child_line_items,
fob_price,

current_timestamp() as ingestion_timestamp,
 




from source as packages_li
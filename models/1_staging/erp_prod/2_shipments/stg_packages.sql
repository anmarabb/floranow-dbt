With source as (
 select * from {{ source(var('erp_source'), 'packages') }}
)
select 

id,
shipment_id,
number,
sequential_id,
barcode,

name,

created_at,
updated_at,


package_type, --0, null
status, -- PENDING, INSPECTED, WAREHOUSED, null
created_by,

items_packing_type, --0, 1, null

items_packing_keys,
fulfillment,
packing_errors,
sub_master_shipment_id,

current_timestamp() as ingestion_timestamp,
 




from source as packages
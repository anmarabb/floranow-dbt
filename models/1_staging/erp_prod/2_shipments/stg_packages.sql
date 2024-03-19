With source as (
 select * from {{ source(var('erp_source'), 'packages') }}
)
select 

id as package_id,
shipment_id,
number,
sequential_id,
barcode,

name,

created_at,
updated_at,

fulfillment, --SUCCEED, FAILED, PARTIAL, UNACCOUNTED, null


package_type, --0, null
status, 
    -- PENDING: initial state
    --INSPECTED: receive package line items
    --WAREHOUSED: add package line items to location
    --null
    --The status for package line items reflect the status for the package.


created_by,

items_packing_type, --0, 1, null

items_packing_keys,

packing_errors,
sub_master_shipment_id,

current_timestamp() as ingestion_timestamp,
 




from source as packages
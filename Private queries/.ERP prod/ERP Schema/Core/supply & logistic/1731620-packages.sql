pac.id,
pac.package_type, --0, null
pac.status, --INSPECTED, PENDING, WAREHOUSED

pac.shipment_id,
pac.number,
pac.barcode,
pac.sub_master_shipment_id,
pac.sequential_id,


--date
    pac.created_at,	
    pac.updated_at,	


pac.created_by,
pac.items_packing_type, --1,0,null
pac.name,
pac.items_packing_keys,
pac.fulfillment, --FAILED, UNACCOUNTED, PARTIAL, SUCCEED
pac.packing_errors,

from `floranow.erp_prod.packages` pac
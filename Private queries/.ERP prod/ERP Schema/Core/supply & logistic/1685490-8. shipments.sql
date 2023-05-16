id,	
name as shipments_name,

supplier_id,
warehouse_id,
customer_id,
master_shipment_id,
canceled_by_id,


status, ---INSPECTED, WAREHOUSED, PACKED, DRAFT, CANCELED
fulfillment, --FAILED, PARTIAL, SUCCEED, UNACCOUNTED
receiving_way,	 --BY_CLIENT, BY_BOX, null

--date
    departure_date,
    created_at,
    updated_at,
    canceled_at,	
    deleted_at,
    received_at,


--quantity
    total_quantity,
    total_received_quantity,
    total_missing_quantity,
    total_damaged_quantity,

total_fob,
total_received_fob,
total_missing_fob,
total_damaged_fob,

invoice_amount,
proforma_amount,


packing_type,
customer_type,
previous_masters,	
invoice_uploaded_by,
proforma_uploaded_by,
cancellation_reason,			
number,	
orders_list_status,

shipping_boxes_count,
warehousing_boxes_count,
	
note,

form `floranow.erp_prod.shipments` as sh
select

msh.id,
msh.name,

msh.origin, --CO, KE, EC, ET, MY, TH, LK, NL, SA, AE, ZA

msh.warehouse_id,
msh.customer_id,


msh.order_sequence,

--date
    msh.created_at,	
    msh.updated_at,	
    msh.departure_date,
    msh.canceled_at,	
    msh.deleted_at,	
    msh.arrival_time,



msh.clearance_cost,		
msh.freight_cost,		
msh.master_invoice_cost,		
msh.freight_currency,
msh.clearance_currency,
msh.master_invoice_currency,

msh.total_fob,
    msh.total_fob.eur,
    msh.total_fob.usd,
    msh.total_fob.aed,
    msh.total_fob.sar,


msh.destination,
msh.status, --PACKED, DRAFT, OPENED, CANCELED, INSPECTED, WAREHOUSED
msh.total_quantity,		
msh.customer_type, --fob, cif, null
msh.fulfillment, --UNACCOUNTED, SUCCEED, PARTIAL
msh.cancellation_reason,
msh.canceled_by_id,
msh.note,


FROM `floranow.erp_prod.master_shipments` as msh
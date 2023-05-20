select 
ii.meta_data.id as meta_id,
ii.meta_data.number as meta_number,
ii.meta_data.invoice_number as meta_invoice_number,
ii.meta_data.order_number as meta_order_number,
ii.meta_data.parcel_number as meta_parcel_number,

ii.meta_data.created_at as meta_created_at,
ii.meta_data.invoice_date as meta_invoice_date,
ii.meta_data.parcel_date as meta_parcel_date,
ii.meta_data.delivery_date as meta_delivery_date,
ii.meta_data.order_date as meta_order_date,

from `floranow.erp_prod.invoice_items` as ii

where source_type is null



---------
date
    ii.created_at,
    ii.updated_at,
    ii.deleted_at,
    ii.order_date,	
    ii.delivery_date,


ii.price_without_tax,
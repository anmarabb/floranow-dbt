select
product_location_id,
product_id,
Product,
product_category,
product_subcategory,
departure_date,
days_until_expiry,

warehouse,
Location,

product_link,
product_location_link,

order_type,
Supplier,
stem_length,

location_quantity,
location_remaining_quantity,

remaining_quantity,

sold_quantity,
incident_quantity_inventory_dmaged,
incident_quantity_inventory_stage,


live_stock,
report_filter,
full_stock_name,

from  {{ref('int_product_locations')}} as pl
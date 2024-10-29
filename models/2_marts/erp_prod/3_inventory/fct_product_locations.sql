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
case 
when Location = 'A1 - X' then 'X-Location'
when Location = 'X - FN' then 'Floranow Location' end as type,
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
Reseller,

live_stock,
report_filter,
full_stock_name,
locationable_id,

missing_quantity,
damaged_quantity,
extra_quantity,
number,
created_at,
updated_at,
locationable_type,
product_color

from  {{ref('int_product_locations')}} as pl

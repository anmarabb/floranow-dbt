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
order_type,
Supplier,


location_quantity,
location_remaining_quantity,

remaining_quantity,
from  {{ref('int_product_locations')}} as pl
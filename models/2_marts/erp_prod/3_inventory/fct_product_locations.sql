select
product_location_id,
product_id,
Product,
warehouse,
Location,

location_quantity,
location_remaining_quantity,

remaining_quantity,
from  {{ref('int_product_locations')}} as pl
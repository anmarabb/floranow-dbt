select
Product,
warehouse,
Location,

location_quantity,
location_remaining_quantity,

from  {{ref('int_product_locations')}} as pl
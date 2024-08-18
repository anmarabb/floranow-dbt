SELECT  
       pi.*, 
       p.Supplier,
       p.warehouse,
       p.Product,
       p.order_type,
       concat(loc.label, " - ", sec.section_name) as Location,
from {{ref ('stg_picked_items')}} pi
left join {{ref ('fct_products')}} p on pi.product_id = p.product_id
left join {{ ref('stg_locations')}} as loc on pi.location_id=loc.location_id
left join {{ ref('stg_sections')}} as sec on sec.section_id = loc.section_id


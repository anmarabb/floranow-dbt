With source as (

    
select
 pl.product_location_id,
 --pl.locationable_id,  --product_id
 p.product_id,
 concat(loc.label, " - ", sec.section_name) as Location,

 p.Product,
 p.product_subcategory,
 p.product_category,
 p.departure_date,
 p.days_until_expiry,
 
 p.warehouse,


 pl.quantity as location_quantity,
 p.remaining_quantity,
 pl.remaining_quantity as location_remaining_quantity,









from {{ ref('stg_product_locations') }} as pl
left join {{ ref('stg_locations')}} as loc on pl.location_id=loc.location_id
left join {{ ref('stg_sections')}} as sec on sec.section_id = loc.section_id

left join {{ ref('fct_products')}} as p on pl.locationable_id = p.product_id


--left join {{ ref('stg_picking_products')}} as pick on pick.product_location_id = pl.product_location_id


where pl.locationable_type = "Product"
 
)
select 

*,

current_timestamp() as ingestion_timestamp,
 
 



from source


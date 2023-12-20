With source as (

    
 select
 p.product_id,
 p.product_name as Product,

 p.warehouse,

pl.locationable_id,
concat(loc.label, " - ", sec.section_name) as Location,

pl.quantity as location_quantity,
pl.remaining_quantity as location_remaining_quantity,



from {{ ref('stg_product_locations') }} as pl
left join {{ ref('stg_locations')}} as loc on pl.location_id=loc.location_id
left join {{ ref('stg_sections')}} as sec on sec.section_id = loc.section_id

left join {{ ref('int_products')}} as p on pl.locationable_id = p.product_id


where pl.locationable_type = "Product"
 
)
select 

*,

current_timestamp() as ingestion_timestamp,
 
 



from source


With source as (

    
select
 pl.product_location_id,
 --pl.locationable_id,  --product_id
 p.product_id,
 concat(loc.label, " - ", sec.section_name) as Location,
concat( "https://erp.floranow.com/products/", p.product_id) as product_link,
concat( "https://erp.floranow.com/product_locations/", pl.product_location_id) as product_location_link,

 p.Product,
 p.product_subcategory,
 p.product_category,
 p.departure_date,
 p.days_until_expiry,
 
 p.warehouse,
 p.order_type,
 p.Supplier,
 p.stem_length,

 pl.quantity as location_quantity,
 pl.remaining_quantity as location_remaining_quantity,

 p.remaining_quantity,
 p.sold_quantity,
 p.incident_quantity_inventory_dmaged,
 p.incident_quantity_inventory_stage,









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


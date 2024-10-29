with product_incidents as (
    select incidentable_id,
           sum(case when incident_type = 'MISSING' then incident_quantity end) as missing_quantity,
           sum(case when incident_type = 'DAMAGED' then incident_quantity end) as damaged_quantity,
           sum(case when incident_type = 'EXTRA' then incident_quantity end) as extra_quantity,
           --sum(case when incident_type = 'CLEANUP_ADJUSTMENTS' then incident_quantity end) as cleanup_adjustments_quantity,
    from  {{ref('int_product_incidents')}}
    where incidentable_type = 'ProductLocation' and deleted_at is null
    group by incidentable_id

),

 source as (

    
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
 pl.locationable_id,
 pl.created_at,
 pl.updated_at,

 p.remaining_quantity,
 p.sold_quantity,
 p.incident_quantity_inventory_dmaged,
 p.incident_quantity_inventory_stage,
 p.Reseller,

p.live_stock,
p.report_filter,
p.full_stock_name,
p.number,

pi.missing_quantity,
pi.damaged_quantity,
pi.extra_quantity, 
pl.locationable_type,
product_color


from {{ ref('stg_product_locations') }} as pl
left join {{ ref('stg_locations')}} as loc on pl.location_id=loc.location_id
left join {{ ref('stg_sections')}} as sec on sec.section_id = loc.section_id

left join {{ ref('fct_products')}} as p on pl.locationable_id = p.product_id

left join product_incidents as pi on pl.product_location_id = pi.incidentable_id
--left join {{ ref('stg_picking_products')}} as pick on pick.product_location_id = pl.product_location_id


where pl.locationable_type = "Product" and pl.deleted_at is null
 
)
select 

*,

current_timestamp() as ingestion_timestamp,
 
 



from source


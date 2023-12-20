With source as (

    
 select 
 

pl.locationable_id,
concat(loc.label, " - ", sec.section_name) as Location,


from {{ ref('stg_product_locations') }} as pl
left join {{ ref('stg_locations')}} as loc on pl.location_id=loc.location_id
left join {{ ref('stg_sections')}} as sec on sec.section_id = loc.section_id

where pl.locationable_type = "Product"
 
)
select 

*,

current_timestamp() as ingestion_timestamp,
 
 



from source


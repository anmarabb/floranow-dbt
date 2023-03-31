with stg_product_locations as (

select 
pl.id as product_location_id,

p.id as product_id,

p.quantity as p_quantity,
pl.quantity as pl_quantity,


p.remaining_quantity as p_remaining_quantity,
pl.remaining_quantity as pl_remaining_quantity,


pl.empty_at as pl_empty_at,
pl.labeled as pl_labeled,


pl.created_at as scaned_to_location_date,

p.created_at as prduct_created_at,

p.expired_at as prduct_expired_at,

pl.locationable_type,


from `floranow.erp_prod.product_locations` as pl
left join `floranow.erp_prod.products` as p on pl.locationable_id = p.id
where p.deleted_at is null 
)


select * from stg_product_locations
where product_id is null
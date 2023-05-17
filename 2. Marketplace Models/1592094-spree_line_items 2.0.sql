with

prep_spp1 as (select * from `floranow.marketplace_prod.spree_product_properties` where __hevo__marked_deleted is false),
prep_spp2 as (select * from `floranow.marketplace_prod.spree_product_properties` where __hevo__marked_deleted is false)

select
distinct p.id,
sli.id,
so.number,
sv.commercial_name as Name,


concat(sp1.presentation ," : " , spp1.value ) as Spec1,
spm.p_one,
concat(sp2.presentation ," : " , spp2.value ) as Spec2,
spm.p_two,


sli.tag_list,
case when sli.florisoft_status = 1 then 'Success' else 'Failed' end as Status,
sli.order_channel,
u.full_name as Customer,
u.debtor_category ,
sr.country_code,
sr.name as Region,
u.florisoft_debtor_number ,

sv1.name as Supplier_Name,
sf.name as Feed_name,



concat (cast(sli.cost_price as string)," ",sli.cost_currency) as FOB_Price,
concat(cast(sli.final_price_object.landed_cost as string)," ",so.currency) as Landed_Cost,
concat(cast(sli.price as string)," ",so.currency) as Final_Price,
concat(cast(round((sli.price - sli.final_price_object.landed_cost),2) as string)," ",so.currency) as markup,
sli.quantity,
concat (so.currency," ",cast ((sli.quantity * sli.price)  as string ))as Total_Price,
sli.delivery_date,
sli.departure_date,
so.completed_at,
 




from `floranow.marketplace_prod.spree_line_items` as sli
left join `floranow.marketplace_prod.spree_orders` as so on sli.order_id = so.id
left join `floranow.marketplace_prod.spree_users` as u on so.user_id = u.id
left join `floranow.marketplace_prod.spree_user_regions` as sur on sur.user_id = u.id
left join `floranow.marketplace_prod.spree_regions` as sr on sr.id = sur.region_id
left join `floranow.marketplace_prod.spree_variants` as sv on sli.variant_id = sv.id
left join `floranow.marketplace_prod.spree_products` as p on sv.product_id = p.id
left join `floranow.marketplace_prod.spree_product_masks` as spm on p.product_mask_id = spm.id
left join `floranow.marketplace_prod.spree_feeds` as sf on p.feed_id = sf.id
left join `floranow.marketplace_prod.spree_vendors` as sv1 on sf.vendor_id =sv1.id

left join `floranow.marketplace_prod.spree_properties` as sp1 on spm.p_one = sp1.floricode_name_id
left join `floranow.marketplace_prod.spree_property_translations` as spt1 on sp1.id = spt1.spree_property_id
left join prep_spp1 as spp1 on spt1.spree_property_id = spp1.property_id and p.id = spp1.product_id

left join `floranow.marketplace_prod.spree_properties` as sp2 on spm.p_two = sp2.floricode_name_id
left join `floranow.marketplace_prod.spree_property_translations` as spt2 on sp2.id = spt2.spree_property_id
left join prep_spp2 as spp2 on spt2.spree_property_id = spp2.property_id and p.id = spp2.product_id
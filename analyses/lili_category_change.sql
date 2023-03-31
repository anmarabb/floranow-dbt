with CTE as (

SELECT

ii.product_name as product,
INITCAP(li.category) as li_category,
INITCAP(ii.category) as ii_category,
li.category2 as item_sub_category_row,

case 
when ii.product_name like '%Lily Ot%' THEN 'Lily Or' 
when ii.product_name like '%Lily Or%' THEN 'Lily Or' 
when ii.product_name like '%Lily La%' THEN 'Lily La' 
else li.category2 end as item_sub_category,


from `floranow.erp_prod.invoice_items`  as ii 
left join `floranow.Floranow_ERP.users` as stg_users on stg_users.id = ii.customer_id
left join `floranow.erp_prod.line_items` as li on ii.line_item_id = li.id
left join `floranow.Floranow_ERP.suppliers` as li_suppliers on li_suppliers.id = li.supplier_id
left join `floranow.erp_prod.invoices` as i on ii.invoice_id = i.id



left join `floranow.erp_prod.order_requests` as orr on li.order_request_id = orr.id



left join `floranow.erp_prod.shipments` as sh on li.shipment_id = sh.id
left join `floranow.erp_prod.proof_of_deliveries` as pod on li.proof_of_delivery_id = pod.id

left join `floranow.erp_prod.feed_sources` as fs on li.feed_source_id = fs.id

left join  `floranow.erp_prod.line_items` as parent_li on parent_li.id = li.parent_line_item_id
left join `floranow.Floranow_ERP.suppliers` as parent_li_suppliers on parent_li_suppliers.id = parent_li.supplier_id

/*
left join `floranow.erp_prod.product_units` as pu on ii.number = pu.number 
left join `floranow.erp_prod.products` as p on p.id = pu.product_id 
left join `floranow.erp_prod.stocks` as stock on p.stock_id = stock .id 
left join floranow.erp_prod.feed_sources as fs1 on fs1.id = p.origin_feed_source_id
left join floranow.erp_prod.feed_sources as fs2 on fs2.id = p.publishing_feed_source_id
left join floranow.erp_prod.feed_sources as fs3 on fs3.id = p.feed_source_id
left join floranow.erp_prod.feed_sources as fs4 on fs4.id = stock.out_feed_source_id
*/

where ii.deleted_at is null
) 

select * 

from CTE

where item_sub_category is not null

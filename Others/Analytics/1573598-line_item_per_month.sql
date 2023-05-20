with 
    prep_last_order_date as (select customer_id,  max(li.created_at) as last_order_date  from `floranow.erp_prod.line_items` as li  group by customer_id ),
    prep_country as (select distinct country_iso_code  as code, country_name from `floranow.erp_prod.country` )
   
SELECT
DATE_TRUNC(li.created_at,month) as month,


count(*) as id_counts,
sum(li.total_price_without_tax) as total_price_without_tax,
--case  when date_diff(cast(li.dispatched_at as date), cast(li.delivery_date as date ), day) > 1 then li.total_price_without_tax else 0 end as transition_sales,


FROM
`floranow.erp_prod.line_items` As li
left join `floranow.erp_prod.shipments` as s on li.shipment_id = s.id
left join `floranow.erp_prod.line_items` as li2 on li.replace_for_id = li2.id
left join `floranow.erp_prod.users` as u on li.customer_id = u.id
left join `floranow.erp_prod.user_categories` as uc on u.user_category_id = uc.id
left join  (select manageable_id,account_manager_id  from `floranow.erp_prod.manageable_accounts` where manageable_type = 'User') manageable_accounts on li.customer_id = manageable_accounts.manageable_id
left join `floranow.erp_prod.account_managers` as am on manageable_accounts.account_manager_id = am.id
left join `floranow.erp_prod.users` as u2 on u2.id = am.user_id
left join `floranow.erp_prod.suppliers` as su on li.supplier_id = su.id
left join `floranow.erp_prod.proof_of_deliveries` as pod on li.proof_of_delivery_id = pod.id
left join `floranow.erp_prod.users` as u3 on pod.customer_id = u3.id
left join `floranow.erp_prod.routes` as r on pod.route_id = r.id
left join `floranow.erp_prod.routes` as r1 on u.route_id = r1.id
left join `floranow.erp_prod.feed_sources` as fs on li.feed_source_id = fs.id
left join `floranow.erp_prod.users` as u4 on li.reseller_id = u4.id
--left join `floranow.erp_prod.feed_sources` as fs1 on fs1.id = li.original_feed_source_id
left join `floranow.erp_prod.warehouses` as w on u.warehouse_id = w.id

left join prep_last_order_date on u.id = prep_last_order_date.customer_id
left join `floranow.erp_prod.invoices` as inv on li.invoice_id = inv.id
left join prep_country as c on u.country = c.code


--where  

group by 1
order by 1 desc
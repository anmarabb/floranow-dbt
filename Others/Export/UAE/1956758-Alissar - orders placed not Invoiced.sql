SELECT
li.order_number,
date(li.created_at) as created_at ,
date(li.delivery_date) as delivery_date,
li.product_name as product,
li.unit_price,
li.quantity,
li.total_price_without_tax,
li.currency,
stg_suppliers.supplier_name,
stg_users.customer,
stg_users.debtor_number,

case 
when li.invoice_id is not null and i.printed_at is null then 'orders placed not Invoiced'
else 'orders Invoiced'
end as order_status,

FROM
`floranow.erp_prod.line_items` As li
left join `floranow.Floranow_ERP.users` as stg_users on stg_users.id = li.customer_id
left join `floranow.Floranow_ERP.suppliers` as stg_suppliers on stg_suppliers.id = li.supplier_id
left join `floranow.erp_prod.proof_of_deliveries` as pod on li.proof_of_delivery_id = pod.id
left join `floranow.erp_prod.shipments` as sh on li.shipment_id = sh.id
left join  `floranow.erp_prod.master_shipments` as msh on sh.master_shipment_id = msh.id
left join `floranow.erp_prod.invoices` as i on li.invoice_id = i.id
left join `floranow.erp_prod.order_requests` as orr on li.order_request_id = orr.id
left join floranow.erp_prod.products AS products ON li.id = products.line_item_id
left join `floranow.erp_prod.feed_sources` as fs on li.feed_source_id = fs.id





where  stg_users.master_account = 'Alissar Flowers'

order by created_at desc

/*
- Would you please support me to run query for us which can include Invoices which are already issued as of now and also the orders placed which are not yet Invoiced.
- This query can be sent on weekly basis which can cover the purchases of the previous week & the order placed already which are not yet Invoiced
*/
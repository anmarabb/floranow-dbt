with 
prep_product_incidents as (select distinct line_item_id, count(*) as incidents_count from `floranow.erp_prod.product_incidents` group by 1  ),
prep_registered_clients as (select financial_administration,count(*) as registered_clients from `floranow.Floranow_ERP.users` where account_type in ('External') group by financial_administration)   
SELECT
i.printed_at,
i.number as invoice_number,
stg_users.customer,
stg_users.city,
ii.product_name,
li.stem_length,
li_suppliers.supplier_name,

case 
when li.parent_line_item_id is not null then parent_li_suppliers.supplier_name 
    when stg_users.financial_administration = 'Bulk' then  ii.meta_data.supplier
    else li_suppliers.supplier_name 
    end as orginal_supplier_name,

case when i.invoice_type = 1 then -ii.quantity else ii.quantity end as quantity,

case when li.parent_line_item_id is not null then parent_li.unit_fob_price else li.unit_fob_price end as unit_fob_price_2,
case when li.parent_line_item_id is not null then parent_li.fob_currency else li.fob_currency end as fob_currency_2,
li.unit_landed_cost,
li.landed_currency,
  ii.unit_price,
  ii.price_without_tax,
  ii.total_tax,
  ii.price,
  ii.currency,
  



stg_users.financial_administration,
stg_users.client_category,
stg_users.account_manager,


case when i.invoice_type = 1 then 'credit note' else 'invoice' end as invoice_type,
i.generation_type,







from `floranow.erp_prod.invoice_items`  as ii 
left join `floranow.Floranow_ERP.users` as stg_users on stg_users.id = ii.customer_id
left join `floranow.erp_prod.line_items` as li on ii.line_item_id = li.id
left join `floranow.erp_prod.invoices` as i on ii.invoice_id = i.id



left join `floranow.Floranow_ERP.suppliers` as li_suppliers on li_suppliers.id = li.supplier_id



left join `floranow.erp_prod.order_requests` as orr on li.order_request_id = orr.id

left join prep_registered_clients as prep_registered_clients on prep_registered_clients.financial_administration = stg_users.financial_administration
left join prep_product_incidents AS product_incidents ON product_incidents.line_item_id = li.id


left join `floranow.erp_prod.shipments` as sh on li.shipment_id = sh.id
left join `floranow.erp_prod.proof_of_deliveries` as pod on li.proof_of_delivery_id = pod.id

left join `floranow.erp_prod.feed_sources` as fs on li.feed_source_id = fs.id

left join  `floranow.erp_prod.line_items` as parent_li on parent_li.id = li.parent_line_item_id
left join `floranow.Floranow_ERP.suppliers` as parent_li_suppliers on parent_li_suppliers.id = parent_li.supplier_id

--left join floranow.erp_prod.products as p on p.line_item_id = li.id
--left join `floranow.erp_prod.stocks` as stock on p.stock_id = stock.id 
--left join `floranow.erp_prod.warehouses` as w on w.id = stock.warehouse_id

left join `floranow.erp_prod.warehouses` as w on w.id = stg_users.warehouse_id

where ii.status = 'APPROVED' and ii.deleted_at is null and  date_diff(cast(current_date() as date ),cast(i.printed_at as date), YEAR) = 0 and stg_users.master_account = 'Alissar Flowers'

--and stg_users.debtor_number = '123654'
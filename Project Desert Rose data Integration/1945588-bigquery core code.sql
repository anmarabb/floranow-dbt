/*
- PostgrasSQL > Hevo > Bigquery > PopSQL > Business Dashboards
- Hevo_time = 5 min
- PopSQL_time = hourly


1- what is the filter that need to apply to extraxt Bloomax project from:
   - invoice
        Abi: where u.warehouse_id = 10
        Samer: 

   - invoice_items
    - Abi: where u.warehouse_id = 10
    - Samer: where ii.deleted_at is null and stg_users.financial_administration = 'KSA' and ii.status = 'APPROVED' and stg_users.city = 'Hail'

*/

-- this is the core code for invoice

with 
prep_registered_clients as (select financial_administration,count(*) as registered_clients from `floranow.Floranow_ERP.users` where account_type in ('External') group by financial_administration)   
SELECT

*

from `floranow.erp_prod.invoices` as i
left join `floranow.Floranow_ERP.users` as stg_users on stg_users.id = i.customer_id
left join prep_registered_clients as prep_registered_clients on prep_registered_clients.financial_administration = stg_users.financial_administration

left join `floranow.Floranow_ERP.stg_paymnets` as stg_paymnets on stg_paymnets.invoice_id = i.id
left join `floranow.Floranow_ERP.stg_invoice_items` as stg_invoice_items on stg_invoice_items.invoice_id = i.id;



--this the core code for invoice_items

with 
prep_product_incidents as (select distinct line_item_id, count(*) as incidents_count from `floranow.erp_prod.product_incidents` group by 1  ),
prep_registered_clients as (select financial_administration,count(*) as registered_clients from `floranow.Floranow_ERP.users` where account_type in ('External') and deleted_accounts != 'Deleted' group by financial_administration)   
SELECT

*

from `floranow.erp_prod.invoice_items`  as ii 
left join `floranow.Floranow_ERP.users` as stg_users on stg_users.id = ii.customer_id
left join `floranow.erp_prod.line_items` as li on ii.line_item_id = li.id
left join `floranow.Floranow_ERP.suppliers` as stg_suppliers on stg_suppliers.id = li.supplier_id
left join `floranow.erp_prod.invoices` as i on ii.invoice_id = i.id

left join `floranow.erp_prod.order_requests` as orr on li.order_request_id = orr.id

left join prep_registered_clients as prep_registered_clients on prep_registered_clients.financial_administration = stg_users.financial_administration
left join prep_product_incidents AS product_incidents ON product_incidents.line_item_id = li.id


left join `floranow.erp_prod.shipments` as sh on li.shipment_id = sh.id
left join `floranow.erp_prod.proof_of_deliveries` as pod on li.proof_of_delivery_id = pod.id

left join `floranow.erp_prod.feed_sources` as fs on li.feed_source_id = fs.id

left join  `floranow.erp_prod.line_items` as parent_li on parent_li.id = li.parent_line_item_id
left join `floranow.Floranow_ERP.suppliers` as parent_li_suppliers on parent_li_suppliers.id = parent_li.supplier_id

where ii.deleted_at is null;
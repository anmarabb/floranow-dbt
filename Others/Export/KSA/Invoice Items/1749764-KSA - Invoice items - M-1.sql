SELECT

case --financial ID
        when i.financial_administration_id = 1 then 'KSA'
        when i.financial_administration_id = 2 then 'UAE'
        when i.financial_administration_id = 3 then 'Jordan'
        when i.financial_administration_id = 4 then 'kuwait'
        when i.financial_administration_id = 5 then 'Qatar'
        when i.financial_administration_id = 6 then 'Bulk'
        when i.financial_administration_id = 7 then 'Internal'
        else 'check_my_logic'
        end as financial_administration,
        
datei.printed_at,
stg_users.debtor_number,
stg_users.customer,
stg_users.client_category,
stg_users.city,
stg_users.account_manager,
case when i.invoice_type = 1 then 'credit note' else 'invoice' end as invoice_type,
i.number as invoice_number,
ii.product_name,
case when ii.meta_data.supplier_name is null then stg_suppliers.supplier_name else ii.meta_data.supplier_name end as supplier,
ii.quantity,
ii.unit_price,
li.unit_landed_cost,
ii.price_without_tax,
ii.total_tax,
ii.price,
ii.currency,
CASE when ii.source_type = 'INTERNAL' then 'ERP' when ii.source_type is null  then 'Florisft' else  'check_my_logic' END AS source_type,
i.generation_type,

case 
when stg_suppliers.supplier_name = 'ASTRA Farms' then 'Astra'
when ii.meta_data.supplier_name in ('Astra Farm','Astra farm Barcode') then 'Astra'
when stg_suppliers.supplier_name = 'Fulfilled by Floranow SA' and fs.name in ('Express Jeddah','Express Dammam','Express Riyadh','Express Tabuk')  then 'Astra'
else 'Non Astra'
end as sales_source,

stg_users.company_name,






from `floranow.erp_prod.invoice_items`  as ii 
left join `floranow.Floranow_ERP.users` as stg_users on stg_users.id = ii.customer_id
left join `floranow.erp_prod.line_items` as li on ii.line_item_id = li.id
left join `floranow.Floranow_ERP.suppliers` as stg_suppliers on stg_suppliers.id = li.supplier_id
left join `floranow.erp_prod.invoices` as i on ii.invoice_id = i.id
left join `floranow.erp_prod.order_requests` as orr on li.order_request_id = orr.id
left join `floranow.erp_prod.shipments` as sh on li.shipment_id = sh.id
left join `floranow.erp_prod.feed_sources` as fs on li.feed_source_id = fs.id


--MTD
where i.financial_administration_id = 1 and ii.status = 'APPROVED' and ii.deleted_at is null and  date_diff(cast(i.printed_at as date), cast(current_date() as date ), MONTH) = 1
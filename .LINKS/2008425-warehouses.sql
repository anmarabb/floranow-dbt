from `floranow.erp_prod.line_items` as li
left join floranow.erp_prod.products as p on p.line_item_id = li.id
left join `floranow.erp_prod.stocks` as stock on p.stock_id = stock.id 
left join `floranow.erp_prod.warehouses` as w on w.id = stock.warehouse_id




from `floranow.erp_prod.invoice_items`  as ii
left join `floranow.Floranow_ERP.users` as stg_users on stg_users.id = ii.customer_id
left join `floranow.erp_prod.warehouses` as w on w.id = stg_users.warehouse_id



from `floranow.erp_prod.product_incidents` as pi 
left join `floranow.erp_prod.line_items` as li on pi.line_item_id = li.id
left join `erp_prod.products` as p on p.line_item_id = li.id 
left join `floranow.erp_prod.stocks` as stock on p.stock_id = stock.id 
left join `floranow.erp_prod.warehouses` as w on w.id = stock.warehouse_id



from floranow.erp_prod.products as p
left join `floranow.erp_prod.stocks` as st on p.stock_id = st.id and p.reseller_id = st.reseller_id
left join `floranow.erp_prod.warehouses` as w on w.id = st.warehouse_id




from `floranow.erp_prod.shipments` as sh
left join  `floranow.erp_prod.master_shipments` as msh on sh.master_shipment_id = msh.id
left join `floranow.erp_prod.warehouses` as w on msh.warehouse_id = w.id
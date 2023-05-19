select count(*) as row_count from floranow_erp_db.public.invoice_items as ii;


select count(*) as row_count from floranow_erp_db.public.line_items as li;

select count(*) as row_count from floranow_erp_db.public.order_requests as orr;


select count(*) as row_count 

from floranow_erp_db.public.invoice_items as ii
left join invoices on ii.invoice_id = invoices.id
left join users u on invoices.customer_id = u.id
where u.warehouse_id = 10
;


select count(*) as row_count 

from floranow_erp_db.public.invoice_items as ii
left join invoices on ii.invoice_id = invoices.id
left join users u on invoices.customer_id = u.id
where u.warehouse_id != 10
;






select 
count(*) as row_count 

from  floranow_erp_db.public.invoice_items   as ii
left join floranow_erp_db.public.users as u on u.id = ii.customer_id
left join floranow_erp_db.public.invoices as i on ii.invoice_id = i.id

where 
  ii.deleted_at is null 
  and
  ii.status = 'APPROVED'

;
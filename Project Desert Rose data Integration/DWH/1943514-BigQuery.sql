select
count(*)
FROM `floranow.erp_prod_2.users`;

select count(*) as row_count from floranow.erp_prod.line_items as li;
select count(*) as row_count from floranow.erp_prod.invoice_items as ii;



select count(*) as row_count from  
`floranow.erp_prod.invoice_items`  as ii 
 left join `floranow.erp_prod.invoices` as invoices on ii.invoice_id = invoices.id
left join `floranow.erp_prod.users` u on invoices.customer_id = u.id
where u.warehouse_id = 10
;

select count(*) as row_count from  
`floranow.erp_prod.invoice_items`  as ii 
 left join `floranow.erp_prod.invoices` as invoices on ii.invoice_id = invoices.id
left join `floranow.erp_prod.users` u on invoices.customer_id = u.id
where u.warehouse_id != 10
;



select
    id,
    count(1) as row_count,
    max(created_at) as created_at,
    max(TIMESTAMP_MILLIS(__hevo__ingested_at)) as hevo_ingested_at,

from
    `floranow.erp_prod.invoice_items`
group by
    id
having
    count (1) > 1
order by
    id;
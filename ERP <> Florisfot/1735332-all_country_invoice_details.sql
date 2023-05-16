select 

round(sum(case when fii.invoice_date = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) then fii.total_revenue else 0 end)) as sales_invoice_date,

from `floranow.florisoft.all_country_invoice_details` fii
;


SELECT

sum(case when date(ii.meta_data.invoice_date) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) then ii.price_without_tax else 0 end) as meta_invoice_date,

/*
sum(case when i.printed_at = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) then ii.price_without_tax else 0 end) as sales_i_printed_at,

sum(case when ii.delivery_date = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) then ii.price_without_tax else 0 end) as sales_ii_delivery_date,

*/


FROM `floranow.erp_prod.invoice_items` AS ii
LEFT JOIN `floranow.erp_prod.invoices` AS i ON ii.invoice_id = i.id


WHERE
  ii.status = 'APPROVED'
  AND ii.source_type IS NULL
  AND ii.deleted_at is null
;



select 

date(fii.PrintedAtTest) as horderKP_VERTREKDAG,
date(fii.invoice_date) as horder_fctdat,

date(fii.delivery_date) as horder_ORDDAT,


from `floranow.florisoft.all_country_invoice_details` fii

;


select 
date(DATE_TRUNC(fii.PrintedAtTest,month)) as print_month,

--date(DATE_TRUNC(fii.PrintedAtTest,month)) as print_date,   --HORDERKP.VERTREKDAG
round(SUM(total_revenue)) as invoiced_revenue,

from `floranow.florisoft.all_country_invoice_details` fii

;


select 
date(DATE_TRUNC(fii.PrintedAtTest,month)) as print_month,

--date(DATE_TRUNC(fii.PrintedAtTest,month)) as print_date,   --HORDERKP.VERTREKDAG
round(SUM(total_revenue)) as invoiced_revenue,

from `floranow.florisoft.all_country_invoice_details` fii
where fii.client_name not like '%Encoding%' 
group by 1 order by 1 desc


--and fii.FinancialAdmin in (9,10,11,12,13)
;

FinancialAdmin,	--DEBITEUR.FINADMIN as FinancialAdmin,

delivery_date, --horder.ORDDAT as DeliveryDate,
invoice_date,	--horder.fctdat InvoiceDate,
PrintedAtTest,	--horderKP.VERTREKDAG as PrintedAtTest

total_revenue,	--SUM (levtotaal * VERKBEDRAG) as InvoiceTotalWithoutVAT,

invoice_id,	

invoice_detail_id,	
client_id,	
drop_id,	
client_name,
samer_client_name,
client_city,
client_country,
client_category,
supplier_id,
supplier_name_row,
supplier_name,
supplier_region,


item_code,
item_name,

item_category,
samer_item_category,


invoice_type,
row_dd_ordnr,	

item_s1,	
total_fob_price,	
unit_fob_price,	
total_lande_price,	
unit_lande_price,	

item_quantity,	
unit_margin,	
unit_selling_price,	
delivery_date_id,	
samer_account_executive,
row_client_city,
row_account_executive,
team_leader,
--with BMX

select
EXTRACT(year FROM invoice_header_printed_at) AS year,
EXTRACT(month FROM invoice_header_printed_at) AS month,
invoice_header_printed_at,
financial_administration,
company_name,
--company_name,
 
 SUM(CASE WHEN  invoice_header_printed_at >= '2022-01-01' AND invoice_header_printed_at < '2023-01-01' THEN gross_revenue ELSE 0 END) AS gross_revenue_2022,
 SUM(CASE WHEN  invoice_header_printed_at >= '2023-01-01' AND invoice_header_printed_at < '2024-01-01' THEN gross_revenue ELSE 0 END) AS gross_revenue_2023,



from {{ref('fct_invoices')}} as i

--where financial_administration = 'KSA'

--where month_printed_date in ('2023-10-01','2022-10-01') and financial_administration = 'KSA'

group by 1,2,3,4,5
order by 1,2,3,4,5 desc


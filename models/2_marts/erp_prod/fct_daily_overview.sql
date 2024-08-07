
with invoices_daily as (

        SELECT
        date(i.invoice_header_printed_at) as invoice_header_printed_at,
        i.warehouse,
        i.financial_administration,

        COALESCE(SUM(i.gross_revenue + i.credit_note), 0) as Sales,
        FROM {{ ref('fct_invoices') }} i
        where i.reprot_filter = 'Floranow Sales'
        GROUP BY 1,2,3

        ),

     incidents_daily as (

        SELECT
        date(pi.incident_at) as incident_at,
        pi.warehouse,
        case when pi.financial_administration = "Internal" and pi.warehouse = 'Dubai Warehouse' then "UAE" else pi.financial_administration end as financial_administration,

        COALESCE(SUM(pi.incident_cost_inventory_dmaged), 0) as Dmaged,
        FROM {{ ref('fct_product_incidents') }} pi
        where master_report_filter = 'inventory_dmaged' --and date(pi.incident_at) = '2024-01-1'
        --and Supplier != 'ASTRA Farms'
        GROUP BY 1,2,3

        )
SELECT 
    d.date,
    d.warehouse,
    d.financial_administration,

    COALESCE(SUM(i.Sales), 0) as Sales, 
    COALESCE(SUM(pi.Dmaged), 0) as Dmaged,


FROM  {{ ref('stg_daily_overview') }} as d
left JOIN  incidents_daily as pi ON date(d.date) = date(pi.incident_at) and d.warehouse =  pi.warehouse and d.financial_administration =pi.financial_administration
left JOIN  invoices_daily as i ON date(d.date) = date(i.invoice_header_printed_at) and d.warehouse = i.warehouse and d.financial_administration = i.financial_administration

GROUP BY 
    1,2,3

ORDER BY 
    1

with invoices_daily as (

        SELECT
        date(i.invoice_header_printed_at) as invoice_header_printed_at,
        i.warehouse,
        i.financial_administration,

        COALESCE(SUM(i.gross_revenue + i.credit_note), 0) as Sales,

        COALESCE(SUM(i.auto_gross_revenue), 0) as auto_gross_revenue,
        COALESCE(SUM(i.auto_credit_note), 0) as auto_credit_note,
        COALESCE(SUM(i.total_cost), 0) as total_cost,

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

        ),
     
     product_quantity as (

      select 
      current_date() as date,
      p.warehouse,
      p.financial_administration,

      COALESCE(SUM(in_stock_quantity), 0) as in_stock_quantity,
      COALESCE(SUM(remaining_value), 0) as inventory_value,

      from {{ref("fct_products")}} p
      where report_filter is not null and stock_model_details in ('Reselling', 'Internal - Riyadh Project X', 'Internal - Dammam Project X', 'Commission Based - Astra Express')

      group by 1, 2, 3

     )

SELECT 
    d.date,
    d.warehouse,
    d.financial_administration,

    COALESCE(SUM(i.Sales), 0) as Sales, 
    COALESCE(SUM(pi.Dmaged), 0) as Dmaged,


    COALESCE(SUM(i.auto_gross_revenue), 0) as auto_gross_revenue,
    COALESCE(SUM(i.auto_credit_note), 0) as auto_credit_note,
    COALESCE(SUM(i.total_cost), 0) as total_cost,

    COALESCE(SUM(pq.in_stock_quantity), 0) as in_stock_quantity,
    COALESCE(SUM(pq.inventory_value), 0) as inventory_value


FROM  {{ ref('stg_daily_overview') }} as d
left JOIN  incidents_daily as pi ON date(d.date) = date(pi.incident_at) and d.warehouse =  pi.warehouse and d.financial_administration =pi.financial_administration
left JOIN  invoices_daily as i ON date(d.date) = date(i.invoice_header_printed_at) and d.warehouse = i.warehouse and d.financial_administration = i.financial_administration
left JOIN  product_quantity as pq ON date(d.date) = date(pq.date) and d.warehouse = pq.warehouse and d.financial_administration = pq.financial_administration
GROUP BY 
    1,2,3

ORDER BY 
    1
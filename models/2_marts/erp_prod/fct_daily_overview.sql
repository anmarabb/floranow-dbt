with products as (
    SELECT line_item_id, 
           SUM(in_stock_quantity) as in_stock_quantity,
           SUM(remaining_value) as inventory_value,

    FROM `dbt_prod_dwh.fct_products`
    WHERE report_filter is not null and stock_model_details in ('Reselling', 'Internal - Riyadh Project X', 'Internal - Dammam Project X', 'Commission Based - Astra Express')

    GROUP BY 1
),

product_incidents as (
    SELECT line_item_id,        
           SUM(pi.incident_cost_inventory_dmaged) as Dmaged,

    FROM `dbt_prod_dwh.fct_product_incidents` pi
    WHERE master_report_filter = 'inventory_dmaged' and after_sold = false
    GROUP BY 1
)

SELECT li.supplier_region as Origin,
       li.Supplier as Supplier,
       d.invoice_header_printed_at,
       d.delivery_date,
       d.financial_administration,
       d.warehouse,
       d.line_item_id,
       COALESCE(d.gross_revenue, 0) as gross_revenue, 
       COALESCE(d.credit_note, 0) as credit_note, 
       COALESCE(d.gross_revenue + d.credit_note, 0) as Sales,
       COALESCE(p.in_stock_quantity, 0) as in_stock_quantity,
       COALESCE(p.inventory_value, 0) as inventory_value,
       COALESCE(pi.Dmaged, 0) as Dmaged,
       COALESCE(d.auto_gross_revenue, 0) as auto_gross_revenue,
       COALESCE(d.auto_credit_note, 0) as auto_credit_note,
       COALESCE(d.total_cost, 0) as total_cost,

FROM  {{ ref('stg_daily_overview') }} as d
left join {{ ref('int_line_items') }} li on d.line_item_id = li.line_item_id
left join products p on p.line_item_id = d.line_item_id
left join product_incidents pi on pi.line_item_id = d.line_item_id

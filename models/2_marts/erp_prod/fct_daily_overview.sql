with invoices as (
    SELECT li.line_item_id, 
           SUM(ii.gross_revenue) as gross_revenue, 
           SUM(ii.credit_note) as credit_note, 
           SUM(ii.auto_gross_revenue) as auto_gross_revenue,
           SUM(ii.auto_credit_note) as auto_credit_note,
           SUM(ii.total_cost) as total_cost,

    FROM {{ref ("int_line_items")}} as li
    left join {{ref ("int_line_items")}} cli on li.line_item_id = cli.parent_line_item_id
    left join {{ref ("fct_invoice_items")}} ii on cli.line_item_id = ii.line_item_id

    WHERE inv_items_reprot_filter = 'Floranow Sales'
    GROUP BY 1
),

products as (
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

SELECT d.* ,
       COALESCE(i.gross_revenue, 0) as gross_revenue, 
       COALESCE(i.credit_note, 0) as credit_note, 
       COALESCE(i.gross_revenue + i.credit_note, 0) as Sales,
       COALESCE(p.in_stock_quantity, 0) as in_stock_quantity,
       COALESCE(p.inventory_value, 0) as inventory_value,
       COALESCE(pi.Dmaged, 0) as Dmaged,
       COALESCE(i.auto_gross_revenue, 0) as auto_gross_revenue,
       COALESCE(i.auto_credit_note, 0) as auto_credit_note,
       COALESCE(i.total_cost, 0) as total_cost,

FROM  {{ ref('stg_daily_overview') }} as d
left join invoices i on d.line_item_id = i.line_item_id
left join products p on p.line_item_id = d.line_item_id
left join product_incidents pi on pi.line_item_id = d.line_item_id

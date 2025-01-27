select Product,
       Supplier,
       Origin,
       date(current_date()) as master_date,
       financial_administration,
       warehouse,
       in_stock_quantity as inventory_stock,
       remaining_value as inventory_value,
       0 as Damaged,
       0 as gross_revenue, 
       0 as credit_note, 
       0 as auto_gross_revenue,
       0 as auto_credit_note,
       0 as total_cost,

from {{ref("fct_products")}} p
WHERE report_filter is not null and stock_model_details in ('Reselling', 'Internal - Riyadh Project X', 'Internal - Dammam Project X', 'Commission Based - Astra Express')

UNION ALL

select Product,
       Supplier,
       Origin,
       date(incident_at) as master_date,
       financial_administration,
       warehouse,
       0 as inventory_stock,
       0 as inventory_value,
       incident_cost_inventory_dmaged as Damaged,
       0 as gross_revenue, 
       0 as credit_note, 
       0 as auto_gross_revenue,
       0 as auto_credit_note,
       0 as total_cost,

from {{ref("fct_product_incidents")}} pi
WHERE master_report_filter = 'inventory_dmaged' and after_sold = false

UNION ALL

select Product,
       Supplier,
       Origin,
       date(invoice_header_printed_at) as master_date,
       financial_administration,
       warehouse,
       0 as inventory_stock,
       0 as inventory_value,
       0 as Damaged,
       gross_revenue, 
       credit_note, 
       auto_gross_revenue,
       auto_credit_note,
       total_cost,

from {{ref("stg_daily_overview")}} 
WHERE inv_items_reprot_filter = 'Floranow Sales'
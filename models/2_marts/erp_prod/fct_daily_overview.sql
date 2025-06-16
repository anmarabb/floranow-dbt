select Product,
       Supplier,
       Origin,
       product_subcategory,
       date(current_date()) as master_date,
       financial_administration,
       warehouse,
       null as account_manager,
       null as user_category,
       null as selling_stage,

       in_stock_quantity as inventory_stock,
       remaining_value as inventory_value,
       0 as Damaged,
       0 as gross_revenue, 
       0 as credit_note, 
       0 as auto_gross_revenue,
       0 as auto_credit_note,
       0 as total_cost,
       0 as sales_target,
       0 as payment_amount,
       0 as collection_target,
       0 as target_budget,

from {{ref("fct_products")}} p
WHERE report_filter is not null and stock_model_details in ('Reselling', 'Internal - Riyadh Project X', 'Internal - Dammam Project X', 'Internal - Jeddah Project X', 'Commission Based - Astra Express')

UNION ALL

select Product,
       Supplier,
       Origin,
       product_subcategory,
       date(incident_at) as master_date,
       case when financial_administration = 'Internal' and warehouse = 'Dubai Warehouse' then 'UAE' else financial_administration end as financial_administration,
       warehouse,
       null as account_manager,
       null as user_category,
       selling_stage,

       0 as inventory_stock,
       0 as inventory_value,
       incident_cost_inventory_dmaged as Damaged,
       0 as gross_revenue, 
       0 as credit_note, 
       0 as auto_gross_revenue,
       0 as auto_credit_note,
       0 as total_cost,
       0 as sales_target,
       0 as payment_amount,
       0 as collection_target,
       0 as target_budget,

from {{ref("fct_product_incidents")}} pi
WHERE master_report_filter = 'inventory_dmaged' and after_sold = false

UNION ALL

select Product,
       Supplier,
       Origin,
       product_subcategory,
       date(invoice_header_printed_at) as master_date,
       financial_administration,
       warehouse,
       account_manager,
       user_category,
       selling_stage,

       0 as inventory_stock,
       0 as inventory_value,
       0 as Damaged,
       gross_revenue, 
       credit_note, 
       auto_gross_revenue,
       auto_credit_note,
       total_cost,
       0 as sales_target,
       0 as payment_amount,
       0 as collection_target,
       0 as target_budget,

from {{ref("stg_daily_overview")}} 
WHERE inv_items_reprot_filter = 'Floranow Sales'

UNION ALL

select null as Product,
       null as Supplier,
       null as Origin,
       null as product_subcategory,
       date(date) as master_date,
       financial_administration,
       warehouse,
       account_manager,
       user_category,
       null as selling_stage,

       0 as inventory_stock,
       0 as inventory_value,
       0 as Damaged,
       0 as gross_revenue, 
       0 as credit_note, 
       0 as auto_gross_revenue,
       0 as auto_credit_note,
       0 as total_cost,
       monthly_budget as sales_target,
       0 as payment_amount,
       0 as collection_target,
       0 as target_budget,

from {{ref("fct_budget")}}

UNION ALL

select null as Product,
       null as Supplier,
       null as Origin,
       null as product_subcategory,
       date(master_date) as master_date,
       financial_administration,
       warehouse,
       account_manager,
       user_category,
       null as selling_stage,

       0 as inventory_stock,
       0 as inventory_value,
       0 as Damaged,
       0 as gross_revenue, 
       0 as credit_note, 
       0 as auto_gross_revenue,
       0 as auto_credit_note,
       0 as total_cost,
       0 as sales_target,
       payment_amount,
       0 as collection_target,
       0 as target_budget,

from {{ref("fct_payments")}} 

UNION ALL

select null as Product,
       null as Supplier,
       null as Origin,
       null as product_subcategory,
       date(date) as master_date,
       financial_administration,
       warehouse,
       account_manager,
       null as user_category,
       null as selling_stage,

       0 as inventory_stock,
       0 as inventory_value,
       0 as Damaged,
       0 as gross_revenue, 
       0 as credit_note, 
       0 as auto_gross_revenue,
       0 as auto_credit_note,
       0 as total_cost,
       0 as sales_target,
       0 as payment_amount,
       monthly_budget as collection_target,
       0 as target_budget,

from {{ref("fct_collection")}} 

UNION ALL

select null as Product,
       null as Supplier,
       null as Origin,
       null as product_subcategory,
       DATE_TRUNC(CURRENT_DATE(), MONTH) as master_date,
       financial_administration,
       warehouse,
       null as account_manager,
       null as user_category,
       null as selling_stage,

       0 as inventory_stock,
       0 as inventory_value,
       0 as Damaged,
       0 as gross_revenue, 
       0 as credit_note, 
       0 as auto_gross_revenue,
       0 as auto_credit_note,
       0 as total_cost,
       0 as sales_target,
       0 as payment_amount,
       0 as collection_target,
       target_budget,

from {{ref("dim_customer")}} 
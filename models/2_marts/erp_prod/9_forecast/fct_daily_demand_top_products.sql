-- Daily demand metrics for top 100 products
-- Uses stg_daily_demand_base for the base structure

WITH 
-- Daily sold quantities from child line items
sold_quantity_daily AS (
    SELECT 
        p.warehouse,
        p.Product as product,
        DATE(cli.created_at) as date,
        SUM(cli.ordered_quantity) as sold_qty,
        0 as arrived_qty_daily,
        0 as ordered_qty,
        0 as incident_qty
    FROM {{ ref('fct_products') }} p
    INNER JOIN {{ ref('stg_daily_demand_base') }} base ON base.product = p.Product AND base.warehouse = p.warehouse
    INNER JOIN {{ ref('int_line_items') }} li ON li.line_item_id = p.line_item_id
    INNER JOIN {{ ref('int_line_items') }} cli ON cli.parent_line_item_id = li.line_item_id
    WHERE cli.parent_line_item_id IS NOT NULL
        AND cli.ordered_quantity > 0
        AND cli.created_at IS NOT NULL
    GROUP BY 1, 2, 3
),

-- Daily arrived/fulfilled quantities
arrived_quantity_daily AS (
    SELECT 
        p.warehouse,
        p.Product as product,
        DATE(COALESCE(li.received_at, li.created_at)) as date,
        0 as sold_qty,
        SUM(li.fulfilled_quantity) as arrived_qty_daily,
        0 as ordered_qty,
        0 as incident_qty
    FROM {{ ref('fct_products') }} p
    INNER JOIN {{ ref('stg_daily_demand_base') }} base ON base.product = p.Product AND base.warehouse = p.warehouse
    INNER JOIN {{ ref('int_line_items') }} li ON li.line_item_id = p.line_item_id
    WHERE li.fulfilled_quantity > 0
        AND (li.received_at IS NOT NULL OR li.created_at IS NOT NULL)
    GROUP BY 1, 2, 3
),

-- Daily ordered quantities
ordered_quantity_daily AS (
    SELECT 
        p.warehouse,
        p.Product as product,
        DATE(li.created_at) as date,
        0 as sold_qty,
        0 as arrived_qty_daily,
        SUM(li.ordered_quantity) as ordered_qty,
        0 as incident_qty
    FROM {{ ref('fct_products') }} p
    INNER JOIN {{ ref('stg_daily_demand_base') }} base ON base.product = p.Product AND base.warehouse = p.warehouse
    INNER JOIN {{ ref('int_line_items') }} li ON li.line_item_id = p.line_item_id
    WHERE li.ordered_quantity > 0
        AND li.created_at IS NOT NULL
    GROUP BY 1, 2, 3
),

-- Daily incident quantities
incident_quantity_daily AS (
    SELECT 
        pi.warehouse,
        pi.product,
        DATE(pi.incident_at) as date,
        0 as sold_qty,
        0 as arrived_qty_daily,
        0 as ordered_qty,
        SUM(pi.incident_quantity) as incident_qty
    FROM {{ ref('int_product_incidents') }} pi
    INNER JOIN {{ ref('stg_daily_demand_base') }} base ON base.product = pi.product AND base.warehouse = pi.warehouse
    WHERE pi.incident_quantity > 0
        AND pi.incident_at IS NOT NULL
    GROUP BY 1, 2, 3
),

-- Combine all metrics using UNION
all_metrics AS (
    SELECT * FROM sold_quantity_daily
    UNION ALL
    SELECT * FROM arrived_quantity_daily
    UNION ALL
    SELECT * FROM ordered_quantity_daily
    UNION ALL
    SELECT * FROM incident_quantity_daily
)

-- Final aggregation by date, warehouse, and product
SELECT 
    date,
    warehouse,
    product,
    SUM(sold_qty) as sold_qty,
    0 as remaining_qty_daily,
    SUM(arrived_qty_daily) as arrived_qty_daily,
    SUM(ordered_qty) as ordered_qty,
    SUM(incident_qty) as incident_qty
FROM all_metrics
GROUP BY 1, 2, 3
ORDER BY warehouse, product, date


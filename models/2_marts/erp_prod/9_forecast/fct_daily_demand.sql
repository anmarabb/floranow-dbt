WITH 
-- Combine incidents, sold, and ordered data
data AS (
    -- Incidents data
    SELECT 
        pi.line_item_id,
        DATE(pi.incident_at) AS date,
        SUM(pi.incident_quantity_before_sold) AS incidents,
        0 AS sold,
        0 AS ordered
    FROM {{ ref('stg_product_incidents') }} pi
    WHERE pi.line_item_id IS NOT NULL
        AND pi.incident_at IS NOT NULL
    GROUP BY 1, 2

    UNION ALL

    -- Sold data (from child line items)
    SELECT 
        li.line_item_id,
        DATE(cli.created_at) AS date,
        0 AS incidents,
        SUM(cli.ordered_quantity) AS sold,
        0 AS ordered
    FROM {{ ref('int_line_items') }} li
    INNER JOIN {{ ref('int_line_items') }} cli 
        ON cli.parent_line_item_id = li.line_item_id
    WHERE li.line_item_id IS NOT NULL
        AND cli.created_at IS NOT NULL
    GROUP BY 1, 2

    UNION ALL

    -- Ordered data (from parent line items)
    SELECT 
        li.line_item_id,
        DATE(li.created_at) AS date,
        0 AS incidents,
        0 AS sold,
        SUM(li.ordered_quantity) AS ordered
    FROM {{ ref('int_line_items') }} li
    WHERE li.line_item_id IS NOT NULL
        AND li.created_at IS NOT NULL
    GROUP BY 1, 2
)

-- Final fact table joining staging table with incidents/sold/ordered data
SELECT 
    p.Product AS product,
    p.warehouse,
    li.date,
    SUM(li.incidents) AS incidents,
    SUM(li.sold) AS sold,
    SUM(li.ordered) AS ordered
FROM {{ ref('stg_daily_demand_base') }} p
INNER JOIN data li 
    ON li.line_item_id = p.line_item_id
WHERE p.Product IS NOT NULL
    AND p.warehouse IS NOT NULL
    AND li.date IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY p.warehouse, p.Product, li.date


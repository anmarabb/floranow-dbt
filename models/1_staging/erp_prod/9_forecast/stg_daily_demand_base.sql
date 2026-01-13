WITH 
-- Get top 100 products by total sold quantity
top_100_products AS (
    SELECT 
        p.Product AS product,
    FROM {{ ref('fct_products') }} p
    LEFT JOIN {{ ref('int_line_items') }} li ON li.line_item_id = p.line_item_id
    LEFT JOIN {{ ref('int_line_items') }} cli ON cli.parent_line_item_id = li.line_item_id
    WHERE cli.departure_date IS NOT NULL
        AND cli.ordered_quantity > 0
    GROUP BY p.Product
    ORDER BY SUM(cli.ordered_quantity) DESC
    LIMIT 100
),

products_aggregated AS (
    SELECT
        p.product_id,
        CASE
            WHEN p.Product LIKE '%Rose%' THEN 'Roses'
            WHEN p.Product IN ('Ruscus Hypophyllum (large Leaf)', 'Pistacia Sp. (leaf)') THEN 'Greeneries'
            ELSE ARRAY_AGG(p.product_category IGNORE NULLS ORDER BY CASE WHEN p.product_category IN ('Others', 'Other') THEN 1 ELSE 0 END, p.product_category)[OFFSET(0)]
        END AS product_category
    FROM {{ ref('fct_products') }} p
    -- INNER JOIN top_100_products t100 ON t100.product = p.Product
    GROUP BY p.product_id, p.Product, p.warehouse, p.age
)

-- Select all columns from products filtered by top 100 products with transformations
SELECT 
    p.product_id,
    CASE WHEN p.Product = 'Ruscus Hypophyllum (large Leaf)' THEN 'Ruscus' ELSE p.Product END AS product,
    p.taxon_age,
    pa.product_category,
    p.* EXCEPT(product_id, Product, taxon_age, product_category)
FROM {{ ref('fct_products') }} p
INNER JOIN top_100_products t100 ON t100.product = p.Product
LEFT JOIN products_aggregated pa ON pa.product_id = p.product_id
ORDER BY p.warehouse, p.Product


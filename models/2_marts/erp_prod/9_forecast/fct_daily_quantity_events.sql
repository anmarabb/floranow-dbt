with data as(
    WITH 
-- Union of all quantity events by creation date
quantity_events AS (
    /* 1. ORDERED (parent line items - initial order quantity) */
    SELECT
        p.product_id,
        p.line_item_id,
        DATE(li.created_at) AS event_date,
        SUM(COALESCE(li.quantity, 0) - COALESCE(li.splitted_quantity, 0)) AS ordered_quantity,
        0 AS incident_quantity,
        0 AS returned_quantity,
        0 AS extra_quantity,
        0 AS child_sold_quantity,
        0 AS reserved_quantity,
        0 AS released_quantity
    FROM {{ ref('stg_products') }} p
    INNER JOIN {{ ref('stg_line_items') }} li ON li.line_item_id = p.line_item_id
    WHERE p.line_item_id IS NOT NULL AND li.created_at IS NOT NULL
    GROUP BY p.product_id, p.line_item_id, DATE(li.created_at)

    UNION ALL

    /* 2. INCIDENTS (before sold - subtracts from available quantity) */
    SELECT
        p.product_id,
        p.line_item_id,
        DATE(pi.incident_at) AS event_date,
        0 AS ordered_quantity,
        SUM(COALESCE(pi.incident_quantity_before_sold, 0)) AS incident_quantity,
        0 AS returned_quantity,
        0 AS extra_quantity,
        0 AS child_sold_quantity,
        0 AS reserved_quantity,
        0 AS released_quantity
    FROM {{ ref('stg_products') }} p
    INNER JOIN {{ ref('stg_line_items') }} li ON li.line_item_id = p.line_item_id
    INNER JOIN {{ ref('stg_product_incidents') }} pi ON pi.line_item_id = li.line_item_id
    WHERE p.line_item_id IS NOT NULL AND pi.incident_at IS NOT NULL
    GROUP BY p.product_id, p.line_item_id, DATE(pi.incident_at)

    UNION ALL

    /* 3. RETURNED (child incidents → adds back to parent product) */
    SELECT
        p_parent.product_id,
        p_parent.line_item_id,
        DATE(pi.incident_at) AS event_date,
        0 AS ordered_quantity,
        0 AS incident_quantity,
        SUM(COALESCE(pi.quantity, 0)) AS returned_quantity,
        0 AS extra_quantity,
        0 AS child_sold_quantity,
        0 AS reserved_quantity,
        0 AS released_quantity
    FROM {{ ref('stg_product_incidents') }} pi
    INNER JOIN {{ ref('stg_line_items') }} child ON child.line_item_id = pi.line_item_id
    INNER JOIN {{ ref('stg_products') }} p_parent ON p_parent.line_item_id = child.parent_line_item_id
    WHERE pi.incident_type = 'RETURNED' AND pi.stage = 'DELIVERY' AND pi.status = 'CLOSED' AND pi.incident_at IS NOT NULL AND child.parent_line_item_id IS NOT NULL
    GROUP BY p_parent.product_id, p_parent.line_item_id, DATE(pi.incident_at)

    UNION ALL

    /* 4. EXTRA (subtract from source line item / parent product) */
    SELECT
        p_source.product_id,
        p_source.line_item_id,
        DATE(extra.created_at) AS event_date,
        0 AS ordered_quantity,
        0 AS incident_quantity,
        0 AS returned_quantity,
        SUM(COALESCE(extra.quantity, 0)) AS extra_quantity,
        0 AS child_sold_quantity,
        0 AS reserved_quantity,
        0 AS released_quantity
    FROM {{ ref('stg_line_items') }} extra
    INNER JOIN {{ ref('stg_products') }} p_source ON p_source.line_item_id = extra.source_line_item_id
    WHERE extra.order_type = 'EXTRA' AND extra.created_at IS NOT NULL AND extra.source_line_item_id IS NOT NULL
    GROUP BY p_source.product_id, p_source.line_item_id, DATE(extra.created_at)

    UNION ALL

    /* 5. CHILD/SOLD (child line items - subtracts from parent) */
    SELECT
        p_parent.product_id,
        p_parent.line_item_id,
        DATE(cli.created_at) AS event_date,
        0 AS ordered_quantity,
        0 AS incident_quantity,
        0 AS returned_quantity,
        0 AS extra_quantity,
        SUM(COALESCE(cli.quantity, 0)) AS child_sold_quantity,
        0 AS reserved_quantity,
        0 AS released_quantity
    FROM {{ ref('stg_products') }} p_parent
    INNER JOIN {{ ref('stg_line_items') }} li ON li.line_item_id = p_parent.line_item_id
    INNER JOIN {{ ref('stg_line_items') }} cli ON cli.parent_line_item_id = li.line_item_id
    WHERE p_parent.line_item_id IS NOT NULL AND cli.created_at IS NOT NULL AND cli.parent_line_item_id IS NOT NULL
    GROUP BY p_parent.product_id, p_parent.line_item_id, DATE(cli.created_at)

    UNION ALL

    /* 6. RESERVED (product level - subtracts from available quantity) */
    SELECT
        ri.product_id,
        NULL AS line_item_id,
        DATE(ri.reserved_at) AS event_date,
        0 AS ordered_quantity,
        0 AS incident_quantity,
        0 AS returned_quantity,
        0 AS extra_quantity,
        0 AS child_sold_quantity,
        SUM(COALESCE(ri.net_reserved_quantity, 0)) AS reserved_quantity,
        0 AS released_quantity
    FROM {{ ref('stg_reserved_items') }} ri
    WHERE ri.status IN ('PENDING', 'PARTIALLY_RELEASED', 'PROCESSING') AND ri.reserved_at IS NOT NULL
    GROUP BY ri.product_id, DATE(ri.reserved_at)

    UNION ALL

    /* 7. RELEASED (product level - subtracts from available quantity) */
    SELECT
        rel.product_id,
        NULL AS line_item_id,
        DATE(rel.released_at) AS event_date,
        0 AS ordered_quantity,
        0 AS incident_quantity,
        0 AS returned_quantity,
        0 AS extra_quantity,
        0 AS child_sold_quantity,
        0 AS reserved_quantity,
        SUM(COALESCE(rel.quantity, 0)) AS released_quantity
    FROM {{ ref('stg_released_items') }} rel
    WHERE rel.status = 'PENDING' AND rel.released_at IS NOT NULL
    GROUP BY rel.product_id, DATE(rel.released_at)
)

-- Final aggregation with calculated remaining quantity
    SELECT
        p.product_id,
        p.Product AS product,
        p.warehouse,
        qe.event_date AS date,
        SUM(COALESCE(qe.ordered_quantity, 0)) AS ordered,
        SUM(COALESCE(qe.incident_quantity, 0)) AS incidents,
        SUM(COALESCE(qe.returned_quantity, 0)) AS returned,
        SUM(COALESCE(qe.extra_quantity, 0)) AS extra,
        SUM(COALESCE(qe.child_sold_quantity, 0)) AS sold,
        SUM(COALESCE(qe.reserved_quantity, 0)) AS reserved,
        SUM(COALESCE(qe.released_quantity, 0)) AS released,
    
    FROM {{ ref('fct_products') }} p
    LEFT JOIN quantity_events qe ON qe.product_id = p.product_id
    WHERE p.Product IS NOT NULL AND p.warehouse IS NOT NULL AND qe.event_date IS NOT NULL
    GROUP BY p.product_id, p.Product, p.warehouse, qe.event_date
)

-- Add cumulative remaining quantity
SELECT
    product_id,
    product,
    warehouse,
    date,
    ordered,
    incidents,
    returned,
    extra,
    sold,
    reserved,
    released,
    COALESCE(ordered - incidents - extra - sold + returned - reserved - released,0) as daily_net_change,
    SUM(COALESCE(ordered - incidents - extra - sold + returned - reserved - released,0)) OVER (PARTITION BY product_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_remaining_quantity,
FROM data
where date > '2023-01-01'
ORDER BY warehouse, product, date


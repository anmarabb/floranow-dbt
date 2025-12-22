
-- This model gets the root supplier when products are transferred between accounts
-- It uses origin_line_item_id which points to the first parent line item from a supplier
-- Simple logic: If origin_line_item_id exists, get supplier from that origin line item, otherwise use direct supplier

with line_items_base as (
    select 
        li.line_item_id,
        li.origin_line_item_id,
        li.feed_source_id,
        li.supplier_id,
        fs.feed_source_name,
        s.supplier_name,
        s.supplier_region as origin
    from {{ ref('stg_line_items') }} as li
    left join {{ ref('stg_feed_sources') }} as fs on fs.feed_source_id = li.feed_source_id
    left join {{ ref('base_suppliers') }} as s on s.supplier_id = li.supplier_id
),

-- Get root supplier from origin_line_item_id (first parent from supplier)
-- Note: origin_line_item_id already points to the first parent from a supplier,
-- so we don't need recursive tracing - we just get the supplier from that origin line item
-- Using DISTINCT to ensure no duplication (in case of any data quality issues)
origin_line_item_supplier as (
    select DISTINCT
        li.line_item_id,
        origin_li.supplier_id as root_supplier_id,
        origin_s.supplier_name as root_supplier,
        origin_s.supplier_region as root_origin
    from line_items_base as li
    left join {{ ref('stg_line_items') }} as origin_li on li.origin_line_item_id = origin_li.line_item_id
    left join {{ ref('base_suppliers') }} as origin_s on origin_li.supplier_id = origin_s.supplier_id
    -- No WHERE clause needed - LEFT JOIN handles nulls, and we want all line items in final output
)

select 
    li.line_item_id,
    -- Simple logic: If origin_line_item_id exists, use supplier from that origin line item, otherwise use direct supplier
    -- This ensures one row per line_item_id with accurate root supplier
    coalesce(olis.root_supplier, li.supplier_name) as root_supplier,
    coalesce(olis.root_origin, li.origin) as root_origin,
    li.feed_source_name,
    -- Flag to indicate if root supplier was found from origin_line_item_id
    case 
        when olis.root_supplier is not null then 'From Origin Line Item (first parent from supplier)'
        else 'Direct Supplier'
    end as root_supplier_source
from line_items_base as li
left join origin_line_item_supplier as olis on li.line_item_id = olis.line_item_id
-- Final DISTINCT to ensure no duplication (defensive measure)
QUALIFY ROW_NUMBER() OVER (PARTITION BY li.line_item_id ORDER BY li.line_item_id) = 1


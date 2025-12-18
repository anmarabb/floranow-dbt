with product_visibility as (
    select 
        *
    from {{ ref('stg_product_visibility_unified') }}
),

products as (
    select 
        p.product_id,
        p.line_item_id,
        p.product_name,
        p.remaining_quantity,
        p.visible,
        st.stock_name,
        case 
            when st.stock_name = 'Inventory Stock' and p.remaining_quantity > 0 then p.remaining_quantity 
            else 0 
        end as in_stock_quantity
    from {{ ref('stg_products') }} as p
    left join {{ ref('base_stocks') }} as st on p.stock_id = st.stock_id and p.reseller_id = st.reseller_id
),

line_items as (
    select 
        line_item_id,
        unit_fob_price
    from {{ ref('stg_line_items') }}
)

select 
    pv.*,
    p.product_name as Product,
    p.in_stock_quantity,
    -- li.unit_fob_price,

from product_visibility as pv
left join products as p on pv.erp_product_id = p.product_id
-- left join line_items as li on p.line_item_id = li.line_item_id


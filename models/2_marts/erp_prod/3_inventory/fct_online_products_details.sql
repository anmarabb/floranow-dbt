with online_products_details as (
    select *
    from {{ source(var('erp_source'), 'online_products_details') }} as opd
),

products as (
    select
        p.product_id,
        p.product_name,
        p.product_category,
        p.product_subcategory,
        p.departure_date,
        p.product_expired_at,
        p.reseller_id,
        p.stock_id,
        p.origin_feed_source_id,
        p.color,
        p.product_created_at
    from {{ ref('stg_products') }} as p
)

select
    -- Online Products Details identifiers
    opd.*,
    
    -- Product dimensions (from online_products_details.erp_product_id)
    p.product_name as Product,
    p.product_category,
    p.product_subcategory,
    p.color as product_color,
    
    -- Product dates
    -- p.departure_date,
    p.product_expired_at,
    p.product_created_at,
    
    -- Warehouse information
    w.warehouse_name as warehouse,
    w.financial_administration,
    
    
    -- Stock information
    st.stock_name as Stock,

    
    
    -- Feed Source information
    origin_fs.feed_source_name as origin_feed_source_name



from online_products_details as opd
left join products as p on opd.erp_product_id = p.product_id
left join {{ ref('base_stocks') }} as st on p.stock_id = st.stock_id and p.reseller_id = st.reseller_id
left join {{ ref('base_warehouses') }} as w on st.warehouse_id = w.warehouse_id
left join {{ ref('stg_feed_sources') }} as origin_fs on p.origin_feed_source_id = origin_fs.feed_source_id


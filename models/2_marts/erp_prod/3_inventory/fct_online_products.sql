with online_products as (
    select *
    from {{ source(var('erp_source'), 'online_products') }} as op

),

products as (
    select
        p.product_id,
        p.product_name,
        p.product_category,
        p.product_subcategory,
        p.remaining_quantity,
        p.departure_date,
        p.product_expired_at,
        p.reseller_id,
        p.stock_id,
        p.origin_feed_source_id,
        p.quantity,
        p.color,
        p.stem_length,
        p.product_created_at
    from {{ ref('stg_products') }} as p
)

select
    -- Online Products identifiers
    op.*,
    
    -- Product dimensions
    p.product_name as Product,
    p.product_category,
    p.product_subcategory,
    p.color as product_color,
    p.stem_length,

    
    -- Product quantities and pricing
    p.quantity,
    p.remaining_quantity,
    
    -- Product dates
    p.departure_date,
    p.product_expired_at,
    case 
        when p.product_expired_at is not null 
        then DATE_DIFF(date(p.product_expired_at), CURRENT_DATE(), DAY) 
        else null 
    end as remaining_days_to_expiry,
    p.product_created_at,
    
    -- Warehouse information
    w.warehouse_name as warehouse,
    w.financial_administration,
    
    
    -- Stock information
    st.stock_name as Stock,

    
    
    -- Feed Source information
    origin_fs.feed_source_name as origin_feed_source_name,
    out_fs.feed_source_name as out_feed_source_name



from online_products as op
left join products as p on op.erp_product_id = p.product_id
left join {{ ref('base_stocks') }} as st on p.stock_id = st.stock_id and p.reseller_id = st.reseller_id
left join {{ ref('base_warehouses') }} as w on st.warehouse_id = w.warehouse_id
left join {{ ref('stg_feed_sources') }} as origin_fs on p.origin_feed_source_id = origin_fs.feed_source_id
left join {{ ref('stg_feed_sources') }} as out_fs on st.out_feed_source_id = out_fs.feed_source_id



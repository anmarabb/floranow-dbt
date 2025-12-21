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
        p.origin_feed_source_id,
        p.departure_date,
        p.product_expired_at,
        st.stock_name,
    from {{ ref('stg_products') }} as p
    left join {{ ref('base_stocks') }} as st on p.stock_id = st.stock_id and p.reseller_id = st.reseller_id
),

product_locations_agg as (
    select 
        pl.locationable_id as product_id,
        STRING_AGG(DISTINCT CONCAT(loc.label, " - ", sec.section_name), ", " ORDER BY CONCAT(loc.label, " - ", sec.section_name)) as item_location
    from {{ ref('stg_product_locations') }} as pl
    left join {{ ref('stg_locations') }} as loc on pl.location_id = loc.location_id
    left join {{ ref('stg_sections') }} as sec on sec.section_id = loc.section_id
    where pl.locationable_type = "Product" 
      and pl.deleted_at is null
    group by pl.locationable_id
)

select 
    pv.* EXCEPT(warehouse_name),
    p.product_name as Product,
    w.warehouse_name as warehouse,
    p.departure_date,
    origin_fs.feed_source_name as origin_feed_name,
    pla.item_location,
    case when p.product_expired_at is not null then DATE_DIFF(p.product_expired_at, CURRENT_DATE(), DAY) else null end as remaining_days_to_expiry

from product_visibility as pv
left join products as p on pv.erp_product_id = p.product_id
left join {{ ref('base_warehouses') }} as w on pv.warehouse_id = w.warehouse_id
left join {{ ref('stg_feed_sources') }} as origin_fs on p.origin_feed_source_id = origin_fs.feed_source_id
left join product_locations_agg as pla on p.product_id = pla.product_id


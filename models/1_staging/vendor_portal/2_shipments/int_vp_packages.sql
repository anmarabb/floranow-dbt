with package_line_items_aggregates as (
    select
        package_id,
        count(distinct line_item_id) as total_line_items_count,
        sum(quantity) as total_quantity_in_package
    from {{ ref('stg_vp_package_line_items') }}
    group by package_id
)

select
    p.*,
    
    -- Shipment Information
    s.shipment_name,
    s.state as shipment_state,
    s.departure_date as shipment_departure_date,
    
    -- Package Line Items Aggregation
    pli.total_line_items_count,
    pli.total_quantity_in_package,
    
    -- Utilization Calculation
    case 
        when p.used_space is not null 
        then SAFE_CAST(NULLIF(REGEXP_REPLACE(TRIM(CAST(p.used_space AS STRING)), r'[^0-9.]', ''), '') AS FLOAT64)
        else null
    end as used_space_numeric

from {{ ref('stg_vp_packages') }} as p
left join {{ ref('stg_vp_shipments') }} as s on p.shipment_id = s.shipment_id
left join package_line_items_aggregates as pli on p.package_id = pli.package_id


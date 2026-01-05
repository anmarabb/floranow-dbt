select
    -- Primary Key
    package_id,
    
    -- Foreign Keys
    shipment_id,
    warehouse_id,
    
    -- Identifiers
    box_label,
    package_number,
    package_type,
    package_dimensions,
    sequence_number,
    
    -- Shipment Information
    shipment_name,
    shipment_state,
    shipment_departure_date,
    
    -- Status Flags
    is_packed,
    is_full,
    has_additions,
    
    -- Fact Measures (Quantities & Metrics)
    item_count,
    total_items_count,
    total_items_count_1,
    total_stems_count,
    package_weight,
    used_space,
    used_space_numeric,
    total_line_items_count,
    total_quantity_in_package,
    
    -- Additional Fields
    assigned_additional_debtor_number,
    preview_line_items,
    sticker_preview_line_items,
    packed_by,
    
    -- Dates
    created_at,
    packed_at,
    
    -- Calculated Fields
    case 
        when is_packed = true then 'Packed'
        when is_packed = false then 'Not Packed'
        else 'Unknown'
    end as packing_status,
    
    case 
        when is_full = true then 'Full'
        when is_full = false then 'Not Full'
        else 'Unknown'
    end as fullness_status,
    
    case 
        when used_space_numeric is not null and used_space_numeric > 0 
        then round((used_space_numeric / 100) * 100, 2)
        else null
    end as utilization_percentage,
    
    -- Package Efficiency Metrics
    case 
        when total_quantity_in_package > 0 and total_line_items_count > 0
        then round(total_quantity_in_package / total_line_items_count, 2)
        else null
    end as avg_quantity_per_line_item

from {{ ref('int_vp_packages') }}


select
    -- Primary Key
    package_line_item_id,
    
    -- Foreign Keys
    package_id,
    line_item_id,
    
    -- Package Information
    package_number,
    box_label,
    is_full,
    used_space,
    package_shipment_id,
    
    -- Line Item Information
    product_name,
    line_item_quantity,
    line_item_original_quantity,
    
    -- Fact Measures (from package_line_items)
    quantity as packed_quantity,
    
    -- Calculated Fields
    case 
        when line_item_quantity > 0 and quantity = line_item_quantity then 'Fully Packed'
        when quantity > 0 and quantity < line_item_quantity then 'Partially Packed'
        when quantity = 0 then 'Not Packed'
        else 'Unknown'
    end as packing_completeness,
    
    case 
        when line_item_original_quantity > line_item_quantity 
        then line_item_original_quantity - line_item_quantity
        else 0
    end as line_item_shortage,
    
    case 
        when line_item_quantity > 0 
        then round((quantity / line_item_quantity) * 100, 2)
        else null
    end as packing_percentage

from {{ ref('int_vp_package_line_items') }}


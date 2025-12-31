select
    adj.*,
    
    -- Original Line Item Information (Original Product)
    li.product_name as original_product_name,
    li.original_quantity as original_quantity,
    li.quantity as original_line_item_current_quantity,
    li.order_id,
    li.variation_id as original_variation_id,
    li.variation_number as original_variation_number,
    
    -- Current Product Information (from adjustment variation_id)
    current_li.product_name as current_product_name,
    current_li.variation_id as current_variation_id,
    current_li.variation_number as current_variation_number,
    
    -- Shipment Information
    s.shipment_name,
    s.state as shipment_state,
    
    -- Shortage Calculations
    case 
        when adj.adjustment_type = 'SHORTAGE' then adj.new_quantity
        else 0
    end as shortage_quantity,
    
    -- Shortage Severity
    case 
        when adj.old_quantity > 0 and adj.new_quantity / adj.old_quantity < 0.1 then 'Small'
        when adj.old_quantity > 0 and adj.new_quantity / adj.old_quantity <= 0.3 then 'Medium'
        when adj.old_quantity > 0 and adj.new_quantity / adj.old_quantity > 0.3 then 'Large'
        else null
    end as shortage_severity

from {{ ref('stg_vp_line_item_quantity_adjustments') }} as adj
left join {{ ref('stg_vp_line_items') }} as li on adj.line_item_id = li.line_item_id
left join {{ ref('stg_vp_line_items') }} as current_li on adj.adjustment_id = current_li.caused_by_adjustment_id  
left join {{ ref('stg_vp_shipments') }} as s on adj.shipment_id = s.shipment_id


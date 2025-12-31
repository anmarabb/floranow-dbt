select
    mri.*,
    
    -- Movement Request Information
    mr.movement_type,
    mr.status as movement_request_status,
    mr.source_shipment_id,
    mr.target_shipment_id,
    
    -- Package Information
    p.package_number,
    p.box_label,
    p.shipment_id as package_shipment_id,
    
    -- Line Item Information
    li.product_name,
    li.quantity as line_item_quantity,
    
    -- Days Rescheduled
    case 
        when mri.old_departure_date is not null 
            and mri.new_departure_date is not null
        then date_diff(mri.new_departure_date, mri.old_departure_date, day)
        else null
    end as days_rescheduled

from {{ ref('stg_vp_movement_request_items') }} as mri
left join {{ ref('stg_vp_movement_requests') }} as mr on mri.movement_request_id = mr.movement_request_id
left join {{ ref('stg_vp_packages') }} as p on mri.package_id = p.package_id
left join {{ ref('stg_vp_line_items') }} as li on mri.line_item_id = li.line_item_id

